/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.zeppelin.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KSQLRestService {
  private static final Logger LOGGER = LoggerFactory.getLogger(KSQLRestService.class);

  private static final String KSQL_ENDPOINT = "%s/ksql";
  private static final String QUERY_ENDPOINT = "%s/query";

  private static final String EXPLAIN_QUERY = "EXPLAIN %s";

  private static final String KSQL_V1_CONTENT_TYPE = "application/vnd.ksql.v1+json; charset=utf-8";

  private static final ObjectMapper json = new ObjectMapper();

  private final String ksqlUrl;
  private final String queryUrl;
  private final String baseUrl;


  private final Map<String, BasicKsqlHttpClient> clientCache;

  private final ExecutorService executorService;

  static class KSQLRequest {
    public final String ksql;
    public final Map<String, Object> streamsProperties;

    public KSQLRequest(final String ksql, final Map<String, Object> streamsProperties) {
      String inputQuery = Objects.requireNonNull(ksql, "ksql").trim();
      this.ksql = inputQuery.endsWith(";") ? inputQuery : inputQuery + ";";
      this.streamsProperties = streamsProperties;
    }

    public KSQLRequest(final String ksql) {
      this(ksql, Collections.emptyMap());
    }

    KSQLRequest toExplainRequest() {
      return new KSQLRequest(String.format(EXPLAIN_QUERY, this.ksql), this.streamsProperties);
    }
  }

  static class KSQLResponse {
    final Map<String, Object> row;
    final String finalMessage;
    final String errorMessage;
    final boolean terminal;

    public KSQLResponse(final List<String> fields, final Map<String, Object> row,
                        final String finalMessage, final String errorMessage, boolean terminal) {
      List<Object> columns = (List<Object>) row.getOrDefault("columns", Collections.emptyList());
      this.row = IntStream.range(0, columns.size())
        .mapToObj(index -> new AbstractMap.SimpleEntry<String, Object>(fields.get(index), columns.get(index)))
        .collect(toLinkedHashMap(e -> e.getKey(), e -> e.getValue()));
      this.finalMessage = finalMessage;
      this.errorMessage = errorMessage;
      this.terminal = terminal;
    }

    public KSQLResponse(final List<String> fields, final Map<String, Object> resp) {
      this(fields, (Map<String, Object>) resp.get("row"), (String) resp.get("finalMessage"),
              (String) resp.get("errorMessage"), (boolean) resp.get("terminal"));
    }
  }

  public KSQLRestService(Map<String, Object> props) {
    baseUrl = Objects.requireNonNull(props.get("ksql.url"), "ksql.url").toString();
    ksqlUrl = String.format(KSQL_ENDPOINT, baseUrl);
    queryUrl = String.format(QUERY_ENDPOINT, baseUrl);
    executorService = Executors.newSingleThreadExecutor();
    clientCache = new ConcurrentHashMap<>();
  }

  private static <T, K, U> Collector<T, ?, Map<K,U>>
    toLinkedHashMap(Function<? super T, ? extends K> keyMapper,
                    Function<? super T, ? extends U> valueMapper) {
       return Collectors.toMap(
               keyMapper,
               valueMapper,
               (u, v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); },
               LinkedHashMap::new);
  }

  public void executeQuery(final String paragraphId, final KSQLRequest request,
                           final Consumer<KSQLResponse> callback) throws IOException {
    List<String> fieldNames = getFields(request);
    if (fieldNames.isEmpty()) {
      throw new RuntimeException("Field list is empty");
    }
    BasicKsqlHttpClient client = new BasicKsqlHttpClient.Builder()
            .withUrl(queryUrl)
            .withJson(json.writeValueAsString(request))
            .withType("POST")
            .withHeader("Content-type", KSQL_V1_CONTENT_TYPE)
            .build();
    BasicKsqlHttpClient oldClient = clientCache.put(paragraphId, client);
    if (oldClient != null) {
      oldClient.close();
    }
    client.connectAsync(new BasicKsqlHttpClient.BasicHTTPClientResponse() {
      @Override
      public void onMessage(int status, String message) {
        try {
          Map<String, Object> queryResponse = json.readValue(message, LinkedHashMap.class);
          KSQLResponse resp = new KSQLResponse(fieldNames, queryResponse);
          callback.accept(resp);
          if (resp.terminal || (resp.errorMessage != null && !resp.errorMessage.isEmpty())
                  || (resp.finalMessage != null && !resp.finalMessage.isEmpty())) {
              client.close();
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void onError(int status, String message) {
        try {
          KSQLResponse resp = new KSQLResponse(fieldNames, Collections.singletonMap("errorMessage", message));
          callback.accept(resp);
          client.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  public void closeClient(final String paragraphId) throws IOException {
    BasicKsqlHttpClient toClose = clientCache.remove(paragraphId);
    if (toClose != null) {
      toClose.close();
    }
  }

  private int getStatusCode(final HttpResponse response) {
    return response.getStatusLine().getStatusCode();
  }

  private List<String> getFields(final KSQLRequest request) throws IOException {
    try (BasicKsqlHttpClient client = new BasicKsqlHttpClient.Builder()
            .withUrl(ksqlUrl)
            .withJson(json.writeValueAsString(request.toExplainRequest()))
            .withType("POST")
            .withHeader("Content-type", KSQL_V1_CONTENT_TYPE)
            .build()) {

      List<Map<String, Object>> explainResponseList = json.readValue(client.connect(), List.class);
      Map<String, Object> explainResponse = explainResponseList.get(0);
      Map<String, Object> queryDescription = (Map<String, Object>) explainResponse.getOrDefault("queryDescription", Collections.emptyMap());
      List<Map<String, Object>> fields = (List<Map<String, Object>>) queryDescription.getOrDefault("fields", Collections.emptyList());
      return fields.stream()
              .map(elem -> elem.getOrDefault("name", "").toString())
              .filter(s -> !s.isEmpty())
              .collect(Collectors.toList());

    }
  }

  public int testServer() throws IOException {
    final CloseableHttpClient client = HttpClients.createDefault();
    HttpHead headRequest = new HttpHead(baseUrl);
    return getStatusCode(client.execute(headRequest));
  }

  public void close() {
    executorService.shutdownNow();
    Set<String> keys = clientCache.keySet();
    keys.forEach(key -> {
      try {
        closeClient(key);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static void main(String... args) throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put("ksql.url", "http://localhost:8088");
    KSQLRestService service = new KSQLRestService(config);
    try {
      service.executeQuery("1", new KSQLRequest("SELECT * FROM ORDERS;"), (resp) -> {
        try {
          System.out.println(json.writeValueAsString(resp.row));
        } catch (Exception e) {}
      });
    } catch (Exception e) {}

  }
}
