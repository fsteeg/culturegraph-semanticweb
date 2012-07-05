/**
 * 
 */
package org.culturegraph.semanticweb.example;

import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;

import org.culturegraph.metastream.sink.StreamWriter;
import org.culturegraph.metastream.source.HttpGetter;
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
		
		final HttpGetter httpGetter = new HttpGetter();
		final JenaModel jenaModel = new JenaModel();
		final JenaModelToStream modelToStream = new JenaModelToStream();
		final StreamWriter streamWriter = new StreamWriter(new OutputStreamWriter(System.out));
		
		httpGetter
				.setReceiver(jenaModel)
				.setReceiver(modelToStream)
				.setReceiver(streamWriter);
		
		httpGetter.process(url);
		httpGetter.closeStream();
	}
	
}
