package io.kestra.plugin.tika;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true)
class RunnerTest {
    @Test
    @ExecuteFlow("sanity-checks/all_tika.yaml")
    void all_tika(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(4));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}
