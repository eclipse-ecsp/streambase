package org.eclipse.ecsp.analytics.stream.base;

import org.apache.kafka.streams.KafkaStreams.State;
import org.eclipse.ecsp.analytics.stream.base.offset.OffsetManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link KafkaStateListener}.
 */
public class KafkaStateListenerTest {

    private KafkaStateListener kafkaStateListener;

    @Mock
    private OffsetManager offsetManager;

    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private KafkaStateAgentListener kafkaStateAgentListener;

    /**
     * Sets up the test environment.
     */
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        kafkaStateListener = new KafkaStateListener();
        ReflectionTestUtils.setField(kafkaStateListener, "offsetManager", offsetManager);
        ReflectionTestUtils.setField(kafkaStateListener, "applicationContext", applicationContext);
    }

    @Test
    public void testOnChange_RunningState() {
        // Simulate state change to RUNNING
        kafkaStateListener.onChange(State.RUNNING, State.REBALANCING);

        // Verify OffsetManager setup is called
        verify(offsetManager, times(1)).setUp();

        // Verify KafkaStateAgentListener is invoked
        Map<String, KafkaStateAgentListener> listeners = Collections.singletonMap("listener", kafkaStateAgentListener);
        when(applicationContext.getBeansOfType(KafkaStateAgentListener.class)).thenReturn(listeners);

        kafkaStateListener.onChange(State.RUNNING, State.REBALANCING);
        verify(kafkaStateAgentListener, times(1)).onChange(State.RUNNING, State.REBALANCING);
    }

    @Test
    public void testOnChange_NonRunningState() {
        // Simulate state change to ERROR
        kafkaStateListener.onChange(State.ERROR, State.RUNNING);

        // Verify OffsetManager setup is not called
        verify(offsetManager, never()).setUp();
    }
}
