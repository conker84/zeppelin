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
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.util.InterpreterOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class KafkaKSQLInterpreter extends Interpreter {
  private static final String NEW_LINE = "\n";

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaKSQLInterpreter.class);
  public static final String TABLE_DELIMITER = "\t";

  private InterpreterOutputStream interpreterOutput = new InterpreterOutputStream(LOGGER);

  private final KSQLRestService ksqlRestService;

  private static final ObjectMapper json = new ObjectMapper();

  public KafkaKSQLInterpreter(Properties properties) {
    this(properties, new KSQLRestService(properties.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue()))));
  }

  // VisibleForTesting
  public KafkaKSQLInterpreter(Properties properties, KSQLRestService ksqlRestService) {
    super(properties);
    this.ksqlRestService = ksqlRestService;
  }

  @Override
  public void open() throws InterpreterException {}

  @Override
  public void close() throws InterpreterException {
    ksqlRestService.close();
  }

  private String writeValueAsString(Object data) {
    try {
      return json.writeValueAsString(data);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private void checkResponseErrors(String message) throws IOException {
    if (message != null && !message.isEmpty()) {
      throw new RuntimeException(message);
//      interpreterOutput.getInterpreterOutput().write("%text");
//      interpreterOutput.getInterpreterOutput().write(NEW_LINE);
//      interpreterOutput.getInterpreterOutput().write(message);
    }
  }

  @Override
  public InterpreterResult interpret(String query, InterpreterContext context) throws InterpreterException {
    interpreterOutput.setInterpreterOutput(context.out);
    try {
      interpreterOutput.getInterpreterOutput().flush();
      int responseCode = ksqlRestService.testServer();
      if (responseCode != 200) {
        return new InterpreterResult(InterpreterResult.Code.ERROR, "Ksql Server HTTP response code: " + responseCode);
      }
      interpreterOutput.getInterpreterOutput().write("%table");
      interpreterOutput.getInterpreterOutput().write(NEW_LINE);
      Set<String> header = new LinkedHashSet<>();
      executeQuery(context.getParagraphId(), query, header);
      return new InterpreterResult(InterpreterResult.Code.SUCCESS);
    } catch (IOException e) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    }
  }

  private void executeQuery(final String paragraphId, final String query, Set<String> header) throws IOException {
    AtomicBoolean isFirstLine = new AtomicBoolean(true);
    ksqlRestService
        .executeQuery(paragraphId, new KSQLRestService.KSQLRequest(query), (resp) -> {
          try{
            checkResponseErrors(resp.errorMessage);
            checkResponseErrors(resp.finalMessage);
            LOGGER.info("Append data {}", resp.row);
            if (isFirstLine.get()) {
              isFirstLine.set(false);
              header.addAll(resp.row.keySet());
              interpreterOutput.getInterpreterOutput().write(header.stream()
                      .collect(Collectors.joining(TABLE_DELIMITER)));
              interpreterOutput.getInterpreterOutput().write(NEW_LINE);
            }
            interpreterOutput.getInterpreterOutput().write(resp.row.values().stream()
                    .map(this::writeValueAsString)
                    .collect(Collectors.joining(TABLE_DELIMITER)));
            interpreterOutput.getInterpreterOutput().write(NEW_LINE);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public void cancel(InterpreterContext context) throws InterpreterException {
    LOGGER.info("Trying to cancel paragraphId {}", context.getParagraphId());
    try {
      ksqlRestService.closeClient(context.getParagraphId());
      LOGGER.info("Removed");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FormType getFormType() throws InterpreterException {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) throws InterpreterException {
    return 0;
  }
}
