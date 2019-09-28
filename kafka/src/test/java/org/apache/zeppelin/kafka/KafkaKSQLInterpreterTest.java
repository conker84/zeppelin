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

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class KafkaKSQLInterpreterTest {

  private KafkaKSQLInterpreter interpreter;
  private InterpreterContext context;

  @Before
  public void setUpZeppelin() throws IOException {

    Map<String, Object> props = Collections.singletonMap("ksql.url", "http://localhost:8081");
    Properties p = new Properties();
    p.putAll(props);
    KSQLRestService service = Mockito.mock(KSQLRestService.class);

    Mockito.when(service.testServer()).thenReturn(200);

    Mockito.doAnswer((invocation) -> {
      Consumer<KSQLRestService.KSQLResponse> callback = (Consumer<KSQLRestService.KSQLResponse>) invocation.getArguments()[2];
      IntStream.range(1, 4).forEach(i -> {
        Map<String, Object> map = new HashMap<>();
        map.put("row", Collections.singletonMap("columns", Arrays.asList("value " + i)));
        map.put("terminal", i == 3);
        callback.accept(new KSQLRestService.KSQLResponse(Arrays.asList("fieldName"), map));
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      });
      return null;
    }).when(service).executeQuery(Mockito.any(String.class), Mockito.any(KSQLRestService.KSQLRequest.class), Mockito.any(Consumer.class));

    interpreter = new KafkaKSQLInterpreter(p, service);
    context = InterpreterContext.builder()
        .setInterpreterOut(new InterpreterOutput(null))
        .setParagraphId("ksql-test")
        .build();
  }

  @After
  public void tearDownZeppelin() throws Exception {
    interpreter.close();
  }


  @Test
  public void shouldRenderKSQLRowsAsTable() throws InterpreterException, IOException, InterruptedException {
    // given
    String query = "select * from orders";

    // when
    interpreter.interpret(query, context);

    // then
    String expected = "%table fieldName\n" +
        "\"value 1\"\n" +
        "\"value 2\"\n" +
        "\"value 3\"\n";
    assertEquals(1, context.out.toInterpreterResultMessage().size());
    assertEquals(expected, context.out.toInterpreterResultMessage().get(0).toString());
    assertEquals(InterpreterResult.Type.TABLE, context.out.toInterpreterResultMessage().get(0).getType());
  }

}
