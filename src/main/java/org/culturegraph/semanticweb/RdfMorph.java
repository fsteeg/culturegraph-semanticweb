package org.culturegraph.semanticweb;

import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import org.culturegraph.metamorph.stream.readers.MultiFormatReader;
import org.culturegraph.metastream.pipe.RecordBatcher;
import org.culturegraph.metastream.pipe.RecordBatcher.BatchListener;
import org.culturegraph.semanticweb.sink.JenaModel;
import org.culturegraph.semanticweb.sink.RDFWriter;
import org.culturegraph.semanticweb.sink.RDFWriter.Format;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * Example which reads mab2, pica and marc21 files and converts them to RDF
 * using a {@link JenaWriter}.
 * 
 * @author Markus Michael Geipel
 */
public final class RdfMorph implements BatchListener {

	private static final String NAMESPACES_CONF = "namespaces";
	
	private final MultiFormatReader reader;
	private final RDFWriter rdfWriter;
	private final JenaModel jenaModel = new JenaModel();
	
	private RdfMorph(final String morphDef) throws UnsupportedEncodingException {
		reader = new MultiFormatReader(morphDef);		
		rdfWriter = new RDFWriter(new OutputStreamWriter(System.out, "UTF8"));
		rdfWriter.setFormat(Format.N3);
	}

	private void morph(final String fileName) throws IOException {
		reader.setFormat(getExtention(fileName));
		jenaModel.configure(reader.getMetamorph());
		reader.setReceiver(new RecordBatcher(this, 2L)).setReceiver(jenaModel);
		reader.read(new FileReader(fileName));
		reader.closeResources();
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

	@Override
	public void batchComplete(final RecordBatcher modelBatcher) {
		final Model model = jenaModel.getModel();
		rdfWriter.process(model);
		jenaModel.reset();
	}
}
