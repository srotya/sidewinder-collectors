package io.sidewinder.collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.junit.Test;
import org.mockito.MockitoAnnotations.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;

import com.google.common.collect.ImmutableMap;

import io.sidewinder.collectors.flume.FlumeSink;

@SuppressWarnings({ "deprecation", "unused", "unchecked" })
public class FlumeSinkTest {
	
	private Context context;
	
	private FlumeSink sink;
	
	@Test
	public void testConstructSidewinderData() {
		context = mock(Context.class);
		ImmutableMap<String, String> subProperties = mock(ImmutableMap.class);
		when(subProperties.get("database")).thenReturn("testDb");
		when(subProperties.get("measurement")).thenReturn("testMeasurement");
//		when(subProperties.get("tags")).thenReturn("tag_1, tag_2");
//		when(subProperties.get("fields")).thenReturn("field_1, field_2");
		
		sink = spy(new FlumeSink());
		when(context.getSubProperties("")).thenReturn(subProperties);
		
		sink.configure(context);
		
		Event event = mock(Event.class);
		
		@SuppressWarnings("rawtypes")
		Map<String, String> eventHeaders = new HashMap();
		eventHeaders.put("measurement", "testMeasurement");
		eventHeaders.put("field_field1", "value1");
		eventHeaders.put("field_field2", "value2");
		
		eventHeaders.put("tag_tag1", "prop1");
		eventHeaders.put("tag_tag2", "prop2");
		
		when(event.getHeaders()).thenReturn(eventHeaders);
		
		String res = sink.constructSidewinderData(event);
		assertTrue(res.contains("testMeasurement,tag1=prop1,tag2=prop2 field1=value1,field2=value2"));
	}

}
