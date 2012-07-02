package org.culturegraph.semanticweb.sink;

import java.util.Map;
import java.util.Stack;

import org.culturegraph.metastream.MetastreamException;
import org.culturegraph.metastream.framework.StreamReceiver;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * adds a record to a Jena model.
 * 
 * @author Markus Michael Geipel, Christoph Böhme
 *
 */
public final class JenaModel implements StreamReceiver {
	
	private static final String HTTP = "http://";
	
	private final Model model;
	private final Stack<Resource> resources = new Stack<Resource>();

	private String homePrefix;

	public JenaModel() {
		model = ModelFactory.createDefaultModel();
	}
	
	public JenaModel(final Model model) {
		this.model = model;
	}

	public void setNamespacePrefixes(final Map<String, String> namespaces) {
		model.setNsPrefixes(namespaces);
	}
	
	public Map<String, String> getNamespacePrefixes() {
		return model.getNsPrefixMap();
	}
	
	public void setHomePrefix(final String prefix) {
		homePrefix = prefix;
	}
	
	public String getHomePrefix() {
		return homePrefix;
	}
	
	public Model getModel(){
		return model;
	}
	
	@Override
	public void startRecord(final String identifier) {
		resources.push(model.createResource(homePrefix + identifier));
	}

	@Override
	public void endRecord() {
		resources.pop();
	}

	@Override
	public void startEntity(final String name) {
		final Resource entity = model.createResource();
		resources.peek().addProperty(createProperty(name), entity);
		resources.push(entity);
	}

	@Override
	public void endEntity() {
		resources.pop();
	}

	@Override
	public void literal(final String name, final String value) {
		addProperty(resources.peek(), name, value);
	}

	@Override
	public void reset() {
		// TODO: Check that this is the correct behaviour
		// Nothing to do		
	}
	
	@Override
	public void closeResources() {
		// TODO: Check that this is the correct behaviour
		// Nothing to do
	}

	private void addProperty(final Resource resource, final String name, final String value) {
		if(value.startsWith(HTTP)){
			resource.addProperty(createProperty(name), model.createResource(value));
		}else{
			resource.addProperty(createProperty(name), value);
		}
	}
	
	private Property createProperty(final String name) {
		if (name.startsWith(HTTP)) {
			return model.createProperty(name);
		}
		
		final int cut = name.indexOf(':');
		if (cut > 0) {
			final String prefix = model.getNsPrefixURI(name.substring(0, cut));
			if (prefix != null) {
				return model.createProperty(prefix, name.substring(cut + 1));
			}
		}
		
		throw new MetastreamException("'" + name + "' is not a valid URI");
	}

}
