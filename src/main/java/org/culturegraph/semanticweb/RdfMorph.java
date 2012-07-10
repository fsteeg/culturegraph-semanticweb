package org.culturegraph.semanticweb;

import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.culturegraph.metastream.reader.MultiFormatReader;
import org.culturegraph.metastream.reader.Reader;
import org.culturegraph.semanticweb.pipe.StreamToModel;
import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;
import org.culturegraph.semanticweb.sink.ModelWriter;

/**
 * Example which reads mab2, pica and marc21 files and converts them to RDF
 * using a {@link JenaWriter}.
 * 
 * @author Markus Michael Geipel
 */
public final class RdfMorph {

	private RdfMorph() {
		// nothing
	}

	/**
	 * @param args
	 *            set args[0] to the recordfilename.
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("Usage: RdfMorph FILE MORPHDEF");
			return;
		}
		final Reader reader = new MultiFormatReader(getExtention(args[0]));
		final StreamToModel streamToModel = new StreamToModel(args[1]);
		final ModelWriter rdfWriter = new ModelWriter(new OutputStreamWriter(System.out, "UTF8"));
		rdfWriter.setFormat(Format.TURTLE);
		reader.setReceiver(streamToModel).setReceiver(rdfWriter);
		
		reader.process(new FileReader(args[0]));
		reader.closeStream();

	}

	private static String getExtention(final String fileName) {
		final int dotPos = fileName.lastIndexOf('.');
		if (dotPos < 0) {
			throw new IllegalArgumentException("Extention missing");
		}
		return fileName.substring(dotPos + 1);
	}
}
