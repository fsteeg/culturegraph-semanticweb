/**
 * 
 */
package org.culturegraph.semanticweb.pipe;

import org.culturegraph.metastream.framework.DefaultSender;
import org.culturegraph.metastream.framework.StreamReceiver;
import org.culturegraph.metastream.framework.StreamReceiverPipe;
import org.culturegraph.metastream.sink.StreamBuffer;

/**
 * Replaces the record id with the value of a literal (__ID).
 * 
 * @author Christoph Böhme
 */
public final class IdModifier implements StreamReceiverPipe<StreamReceiver> {

	public static final String ID_LITERAL = "__ID";
	
	/**
	 * Performs the actual replacement of the record id.
	 * 
	 * @author Christoph Böhme
	 */
	private class ModifyStream extends DefaultSender<StreamReceiver>
			implements StreamReceiverPipe<StreamReceiver> {

		@Override
		public void startRecord(final String identifier) {
			if (recordId != null) {
				getReceiver().startRecord(recordId);
			} else {
				getReceiver().startRecord(identifier);
			}
		}

		@Override
		public void endRecord() {
			getReceiver().endRecord();
		}

		@Override
		public void startEntity(final String name) {
			getReceiver().startEntity(name);
		}

		@Override
		public void endEntity() {
			getReceiver().endEntity();
		}

		@Override
		public void literal(final String name, final String value) {
			getReceiver().literal(name, value);
		}

		@Override
		public void close() {
			getReceiver().close();
		}
		
	}

	private final StreamBuffer buffer = new StreamBuffer();
	private final ModifyStream modifyStream = new ModifyStream();
	private String recordId;
		
	public IdModifier() {
		buffer.setReceiver(modifyStream);
	}
	
	@Override
	public <R extends StreamReceiver> R setReceiver(final R receiver) {
		modifyStream.setReceiver(receiver);
		return receiver;
	}	
	
	@Override
	public void startRecord(final String identifier) {
		buffer.startRecord(identifier);
	}

	@Override
	public void endRecord() {
		buffer.endRecord();
		buffer.replay();
		recordId = null;
	}

	@Override
	public void startEntity(final String name) {
		buffer.startEntity(name);
	}

	@Override
	public void endEntity() {
		buffer.endEntity();
	}

	@Override
	public void literal(final String name, final String value) {
		if (ID_LITERAL.equals(name)) {
			recordId = value;
			return;
		} 
		buffer.literal(name, value);
	}

	@Override
	public void close() {
		modifyStream.close();
	}
}