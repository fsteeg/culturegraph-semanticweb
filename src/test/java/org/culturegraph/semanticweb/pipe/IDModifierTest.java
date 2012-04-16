/**
 * 
 */
package org.culturegraph.semanticweb.pipe;

import org.culturegraph.metastream.converter.CGTextDecoder;
import org.culturegraph.metastream.sink.EventList;
import org.culturegraph.metastream.util.StreamValidator;
import org.culturegraph.semanticweb.pipe.IdModifier;
import org.junit.Test;

/**
 * @author Christoph Böhme
 *
 */
public final class IDModifierTest {

	private static final String RECORD3 = "3={ name=test }";
	
	@Test
	public void test() {
		
		final CGTextDecoder decoder = new CGTextDecoder();
		
		final EventList expected = new EventList();
		
		decoder.setReceiver(expected);
		
		decoder.process("1={ name=test, entity={ name=test } }");
		decoder.process("2={ name=test}");
		decoder.process(RECORD3);
		decoder.close();
				
		final IdModifier idModifier = new IdModifier();
		final StreamValidator validator = new StreamValidator(expected.getEvents());
		
		decoder.setReceiver(idModifier);
		idModifier.setReceiver(validator);

		decoder.process("one={ name=test, __ID=1, entity={ name=test } }");
		decoder.process("two={ name=test, __ID=2}");
		decoder.process(RECORD3);
		decoder.close();
	}

}
