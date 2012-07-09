package org.culturegraph.semanticweb.sink;

import java.io.IOException;
import java.io.Writer;

import org.culturegraph.metastream.MetastreamException;

/**
 * @author Christoph Böhme <c.boehme@dnb.de>
 *
 */
public final class ModelWriter extends AbstractModelWriter {

	

	private final Writer writer;
	
	
	public ModelWriter(final Writer writer) {
		super();
		this.writer = writer;
	}

	
	@Override
	protected void doCloseStream() {
		try {
			writer.close();
		} catch (IOException e) {
			throw new MetastreamException(e);
		}
	}


	@Override
	public void resetStream() {
		throw new UnsupportedOperationException("RdfWriter cannot be reset");
	}


	@Override
	protected Writer getWriter() {
		return writer;
	}

}
