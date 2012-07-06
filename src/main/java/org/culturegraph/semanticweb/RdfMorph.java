package org.culturegraph.semanticweb;

import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.culturegraph.metamorph.core.Metamorph;
import org.culturegraph.metamorph.core.MetamorphBuilder;
import org.culturegraph.metamorph.reader.Reader;
import org.culturegraph.metamorph.reader.ReaderFactory;

import org.culturegraph.metastream.pipe.RecordBatcher;
import org.culturegraph.semanticweb.pipe.JenaModel;
import org.culturegraph.semanticweb.sink.RdfWriter;
import org.culturegraph.semanticweb.sink.AbstractRdfWriter.Format;

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
		final Reader reader = new ReaderFactory().newInstance(getExtention(args[0]));
		final Metamorph metamorph = MetamorphBuilder.build(args[1]);
		final JenaModel jenaModel = new JenaModel();
		jenaModel.configure(metamorph);
		final RdfWriter rdfWriter = new RdfWriter(new OutputStreamWriter(System.out, "UTF8"));
		rdfWriter.setFormat(Format.N3);

		reader.setReceiver(metamorph).setReceiver(new RecordBatcher(jenaModel, 2)).setReceiver(jenaModel)
				.setReceiver(rdfWriter);
		
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
