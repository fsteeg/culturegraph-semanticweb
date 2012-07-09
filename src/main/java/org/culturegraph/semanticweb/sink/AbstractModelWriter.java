package org.culturegraph.semanticweb.sink;

import java.io.Writer;

import org.culturegraph.metastream.framework.ObjectReceiver;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * @author Christoph Böhme <c.boehme@dnb.de>, Markus Geipel
 *
 */
public abstract class AbstractModelWriter implements ObjectReceiver<Model> {

	public static enum Format {
		RDF_XML("RDF/XML"), RDF_XML_ABBREV("RDF/XML-ABBREV"), 
		N_TRIPLE("N-TRIPLE"), N3("N3"), TURTLE("TURTLE");
		
		private final String name;
		
		Format(final String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
	}

	
	private Format format = Format.RDF_XML;
	


	public final void setFormat(final Format format) {
		this.format = format;
	}
	
	public final Format getFormat() {
		return format;
	}
	
	@Override
	public final void process(final Model model) {
		model.write(getWriter(), format.getName());
	
	}


	
	protected abstract Writer getWriter();

	@Override
	public final void closeStream() {
		doCloseStream();
	}

	protected void doCloseStream() {
		// nothing by default
	}

}
