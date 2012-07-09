package org.culturegraph.semanticweb.sink;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.culturegraph.metastream.MetastreamException;
import org.culturegraph.metastream.annotation.Description;
import org.culturegraph.metastream.annotation.In;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * @author Markus Geipel
 *
 */

@Description("writes Jena models to RDF files.")
@In(Model.class)
public final class ModelFileWriter extends AbstractModelWriter {

	private FileWriter writer;
	private int count;
	private final String filePrefix;
	
	public ModelFileWriter() {
		super();
		this.filePrefix = "";
	}
	
	public ModelFileWriter(final String filePrefix) {
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
