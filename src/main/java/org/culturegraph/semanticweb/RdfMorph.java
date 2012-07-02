package org.culturegraph.semanticweb;

import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import java.io.Writer;

import org.culturegraph.metamorph.stream.readers.MultiFormatReader;
import org.culturegraph.semanticweb.pipe.ModelBatcher;
import org.culturegraph.semanticweb.sink.RDFWriter;

/**
 * Example which reads mab2, pica and marc21 files and converts them to RDF
 * using a {@link JenaWriter}.
 * 
 * @author Markus Michael Geipel
 */
public final class RdfMorph {

	private static final String NAMESPACES_CONF = "namespaces";
	
	private final MultiFormatReader reader;
	private final ModelBatcher modelBatcher;
	private final RDFWriter rdfWriter;
	
	private RdfMorph(final String morphDef) throws UnsupportedEncodingException {
		reader = new MultiFormatReader(morphDef);		
		modelBatcher = new ModelBatcher();
		rdfWriter = new RDFWriter(new OutputStreamWriter(System.out, "UTF8"));
	}

	private void morph(final String fileName) throws IOException {
		reader.setFormat(getExtention(fileName));
		reader.setReceiver(modelBatcher);
		modelBatcher.setReceiver(rdfWriter);
		
		modelBatcher.setNamespacePrefixes(reader.getMetamorph().getMap(NAMESPACES_CONF));
		modelBatcher.setHomePrefix(reader.getMetamorph().getMap(NAMESPACES_CONF).get(""));
		
		reader.read(new FileReader(fileName));
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
