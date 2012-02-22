package org.culturegraph.semanticweb;

import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

import org.culturegraph.metamorph.stream.readers.MultiFormatReader;
import org.culturegraph.semanticweb.stream.receiver.JenaWriter;
import org.culturegraph.semanticweb.stream.receiver.JenaWriter.BatchFinishedListener;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * Example which reads mab2, pica and marc21 files and converts them to RDF
 * using a {@link JenaWriter}.
 * 
 * @author Markus Michael Geipel
 */
public final class RdfMorph implements BatchFinishedListener {

	private static final String RDF_XML_ABR = "RDF/XML-ABBREV";

	private final JenaWriter jenaWriter = new JenaWriter();
	private final Writer out;
	private final MultiFormatReader reader;

	private RdfMorph(final String morphDef) throws UnsupportedEncodingException {
		out = new OutputStreamWriter(System.out, "UTF8");
		reader = new MultiFormatReader(morphDef);
		jenaWriter.setBatchFinishedListener(this);

	}

	private void morph(final String fileName) throws IOException {

		reader.setFormat(getExtention(fileName));
		reader.setReceiver(jenaWriter);
		jenaWriter.configure(reader.getMetamorph());
		reader.read(new FileReader(fileName));
		onBatchFinished(jenaWriter.getModel());
	}

	@Override
	public void onBatchFinished(final Model model) {
		model.write(out, RDF_XML_ABR);
	}

	/**
	 * @param args
	 *            set args[0] to the recordfilename.
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		final RdfMorph rdfMorph;
		if (args.length == 2) {
			rdfMorph = new RdfMorph(args[1]);
		} else {
			System.err.println("Usage: RdfMorph FILE MORPHDEF");
			return;
		}
		rdfMorph.morph(args[0]);
	}

	private static String getExtention(final String fileName) {
		final int dotPos = fileName.lastIndexOf('.');
		if (dotPos < 0) {
			throw new IllegalArgumentException("Extention missing");
		}
		return fileName.substring(dotPos + 1);
	}

}
