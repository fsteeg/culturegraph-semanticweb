/**
 * 
 */
package org.culturegraph.semanticweb.example;

import java.io.OutputStreamWriter;
import java.net.URISyntaxException;

import org.culturegraph.metastream.sink.StreamWriter;
import org.culturegraph.metastream.source.HttpOpener;
import org.culturegraph.semanticweb.pipe.JenaModel;
import org.culturegraph.semanticweb.pipe.JenaModelToStream;

/**
 * @author Christoph Böhme
 *
 */
public final class RdfReader {

	private static final String EXAMPLE_URL = "http://d-nb.info/gnd/121649091/about/rdf";
	
	private RdfReader() {
		// Nothing to do
	}
	
	public static void main(final String[] args) throws URISyntaxException  {
		String url = EXAMPLE_URL;
		if (args.length >= 1) {
			url = args[0];
		}
		
		final HttpOpener httpOpener = new HttpOpener();
		final JenaModel jenaModel = new JenaModel();
		final JenaModelToStream modelToStream = new JenaModelToStream();
		final StreamWriter streamWriter = new StreamWriter(new OutputStreamWriter(System.out));
		
		httpOpener
				.setReceiver(jenaModel)
				.setReceiver(modelToStream)
				.setReceiver(streamWriter);
		
		httpOpener.process(url);
		httpOpener.closeStream();
	}
	
}
