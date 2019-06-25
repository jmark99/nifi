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
package org.apache.nifi.attribute.expression.language.evaluation.selection;

import java.util.Map;

import org.apache.nifi.attribute.expression.language.evaluation.EvaluatorState;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

public class DelineatedAttributeEvaluator extends MultiAttributeEvaluator {

    private final Evaluator<String> subjectEvaluator;
    private final Evaluator<String> delimiterEvaluator;
    private final int evaluationType;

    public DelineatedAttributeEvaluator(final Evaluator<String> subjectEvaluator, final Evaluator<String> delimiterEvaluator, final int evaluationType) {
        this.subjectEvaluator = subjectEvaluator;
        this.delimiterEvaluator = delimiterEvaluator;
        this.evaluationType = evaluationType;
    }

    @Override
    public QueryResult<String> evaluate(final Map<String, String> attributes, final EvaluatorState context) {
        State state = context.getState(this, State.class);
        if (state == null) {
            state = new State();
            context.putState(this, state);
        }
        if (state.delineatedValues == null) {
            final QueryResult<String> subjectValue = subjectEvaluator.evaluate(attributes, context);
            if (subjectValue.getValue() == null) {
                state.evaluationsLeft = 0;
                return new StringQueryResult(null);
            }

            final QueryResult<String> delimiterValue = delimiterEvaluator.evaluate(attributes, context);
            if (delimiterValue.getValue() == null) {
                state.evaluationsLeft = 0;
                return new StringQueryResult(null);
            }

            state.delineatedValues = subjectValue.getValue().split(delimiterValue.getValue());
        }

        if (state.evaluationCount > state.delineatedValues.length || state.delineatedValues.length == 0) {
            state.evaluationsLeft = 0;
            return new StringQueryResult(null);
        }

        state.evaluationsLeft = state.delineatedValues.length - state.evaluationCount - 1;

        return new StringQueryResult(state.delineatedValues[state.evaluationCount++]);
    }

    @Override
    public Evaluator<?> getLogicEvaluator() {
        return subjectEvaluator;
    }

    @Override
    public int getEvaluationsRemaining(final EvaluatorState context) {
        State state = context.getState(this, State.class);
        if (state == null) {
            state = new State();
            context.putState(this, state);
        }
        return state.evaluationsLeft;
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }

    @Override
    public int getEvaluationType() {
        return evaluationType;
    }

    private class State {
        private String[] delineatedValues;
        private int evaluationCount = 0;
        private int evaluationsLeft = 1;
    }
}
