package org.culturegraph.semanticweb.pipe;

import org.culturegraph.metamorph.core.Metamorph;
import org.culturegraph.metastream.annotation.Description;
import org.culturegraph.metastream.annotation.In;
import org.culturegraph.metastream.annotation.Out;
import org.culturegraph.metastream.framework.DefaultStreamPipe;
import org.culturegraph.metastream.framework.ObjectReceiver;
import org.culturegraph.metastream.framework.StreamReceiver;
import org.culturegraph.metastream.pipe.RecordBatcher;

import com.hp.hpl.jena.rdf.model.Model;

@Description("builds a RDF model from a stream. Give metamorph def in brackets.")
@In(StreamReceiver.class)
@Out(Model.class)
public final class StreamToModel extends DefaultStreamPipe<ObjectReceiver<Model>>{
	
	private static final int DEFAULT_BATCH = 1;
	private final Metamorph metamorph;
	private final JenaModel jenaModel;
	private int batchSize = DEFAULT_BATCH;
	
	public StreamToModel(final String morphDef) {
		super();
		metamorph = new Metamorph(morphDef);
		jenaModel = new JenaModel();
		jenaModel.configure(metamorph);
	}
	
	public void setBatchSize(final int batchSize) {
		this.batchSize = batchSize;
	}
	
	@Override
	protected void onSetReceiver() {
		metamorph.setReceiver(new RecordBatcher(jenaModel, batchSize)).setReceiver(jenaModel).setReceiver(getReceiver());
	}
	
	
}
