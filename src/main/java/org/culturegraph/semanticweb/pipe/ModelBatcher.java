package org.culturegraph.semanticweb.pipe;

import java.util.Map;

import org.culturegraph.metastream.framework.DefaultSender;
import org.culturegraph.metastream.framework.ObjectReceiver;
import org.culturegraph.metastream.framework.StreamReceiverPipe;
import org.culturegraph.semanticweb.sink.JenaModel;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * @author Christoph Böhme <c.boehme@dnb.de>
 *
 */
public final class ModelBatcher extends DefaultSender<ObjectReceiver<Model>> 
	implements StreamReceiverPipe<ObjectReceiver<Model>> {

	public static final int DEFAULT_BATCH_SIZE = 1000;
	
	private final JenaModel writer = new JenaModel();
	
	private int batchSize = DEFAULT_BATCH_SIZE;
	private long count;

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(final int batchSize) {
		this.batchSize = batchSize;
	}

	public void setNamespacePrefixes(final Map<String, String> namespaces) {
		writer.setNamespacePrefixes(namespaces);
	}
	
	public Map<String, String> getNamespacePrefixes() {
		return writer.getNamespacePrefixes();
	}
	
	public void setHomePrefix(final String prefix) {
		writer.setHomePrefix(prefix);
	}
	
	public String getHomePrefix() {
		return writer.getHomePrefix();
	}

	@Override
	public void startRecord(final String identifier) {
		writer.startRecord(identifier);
	}

	@Override
	public void endRecord() {
		writer.endRecord();
		count += 1;
		if (count % batchSize == 0) {
			getReceiver().process(writer.getModel());
			writer.getModel().removeAll();
		}
	}

	@Override
	public void startEntity(final String name) {
		writer.startEntity(name);
	}

	@Override
	public void endEntity() {
		writer.endEntity();
	}

	@Override
	public void literal(final String name, final String value) {
		writer.literal(name, value);
	}

	@Override
	public void close() {
		writer.close();
	}

}
