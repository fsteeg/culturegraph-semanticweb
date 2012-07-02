package org.culturegraph.semanticweb.sink;

import java.io.IOException;
import java.io.Writer;

import org.culturegraph.metastream.MetastreamException;
import org.culturegraph.metastream.framework.ObjectReceiver;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * @author Christoph Böhme <c.boehme@dnb.de>
 *
 */
public final class RDFWriter implements ObjectReceiver<Model> {

	public static enum Format {
		RDF_XML("RDF/XML"), RDF_XML_ABBREV("RDF/XML-ABBREV"), 
		N_TRIPLE("N-TRIPLE"), N3("N3");
		
		private final String name;
		
		Format(final String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
	};

	private final Writer writer;
	
	private Format format = Format.RDF_XML;
	
	public RDFWriter(final Writer writer) {
		this.writer = writer;
	}

	public void setFormat(final Format format) {
		this.format = format;
	}
	
	public Format getFormat() {
		return format;
	}
	
	@Override
	public void process(final Model model) {
		model.write(writer, format.getName());
	}
	
	@Override
	public void reset() {
		throw new UnsupportedOperationException("Cannot reset RDFWriter");
	}
	
	@Override
	public void closeResources() {
		try {
			writer.close();
		} catch (IOException e) {
			throw new MetastreamException(e);
		}
	}

}
