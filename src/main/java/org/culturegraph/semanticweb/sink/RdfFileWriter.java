package org.culturegraph.semanticweb.sink;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.culturegraph.metastream.MetastreamException;

/**
 * @author Markus Geipel
 *
 */
public final class RdfFileWriter extends AbstractRdfWriter {

	

	private FileWriter writer;
	private int count;
	private final String filePrefix;
	
	
	public RdfFileWriter(final String filePrefix) {
		super();
		this.filePrefix = filePrefix;
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
		final String file = String.format(filePrefix + "%2$03d", Integer.valueOf(++count));
		try {
			writer = new FileWriter(file);
		} catch (IOException e) {
			throw new MetastreamException("faile to open '" + file + "'.", e);
		}
		return writer;
	}
}
