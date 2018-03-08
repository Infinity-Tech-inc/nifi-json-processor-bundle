/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nifi.processor.jsonvalidator.processors.jsonprocessor;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class JsonFieldExtractorProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(JsonFieldExtractorProcessor.class);
    }

    @Test
    public void testProcessor() {

        final String testInput = "{\"hello\":\"123\"}";
        testRunner.setProperty(JsonFieldExtractorProcessor.JSON_STRING, "$.hello");
        testRunner.enqueue(testInput.getBytes());
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(JsonFieldExtractorProcessor.SUCCESS);

        MockFlowFile transferredFlowFile = testRunner.getFlowFilesForRelationship(JsonFieldExtractorProcessor.SUCCESS).get(0);
        transferredFlowFile.assertAttributeExists("match");
        transferredFlowFile.assertAttributeEquals("match", "123");
    }

    @Test
    public void testProcessorWithInValidJson() {

        final String testInput = "{\"hello\"::\"123\"}";
        testRunner.setProperty(JsonFieldExtractorProcessor.JSON_STRING, "$.hello");
        testRunner.enqueue(testInput.getBytes());
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(JsonFieldExtractorProcessor.FAILURE);

        MockFlowFile transferredFlowFile = testRunner.getFlowFilesForRelationship(JsonFieldExtractorProcessor.FAILURE).get(0);
        transferredFlowFile.assertAttributeExists("match");
        transferredFlowFile.assertAttributeEquals("match", "");
    }

}
