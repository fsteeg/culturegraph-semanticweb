package org.culturegraph.semanticweb.pipe;

import java.io.Reader;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.culturegraph.metastream.MetastreamException;
import org.culturegraph.metastream.framework.ObjectReceiver;
import org.culturegraph.metastream.framework.Sender;
import org.culturegraph.metastream.framework.StreamReceiver;
import org.culturegraph.metastream.pipe.RecordBatcher;
import org.culturegraph.metastream.pipe.RecordBatcher.BatchListener;
import org.culturegraph.util.SimpleMultiMap;

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
public final class JenaModel implements StreamReceiver, ObjectReceiver<Reader>,
		Sender<ObjectReceiver<Model>>, BatchListener {
	
	public static final String NAMESPACES_CONF = "namespaces";

	private static final String HTTP = "http://";

	private final Model model;
	private final Deque<Resource> resources = new LinkedList<Resource>();

	private String homePrefix;
	
	private ObjectReceiver<Model> receiver;

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

	public Model getModel() {
		return model;
	}

	public void configure(final SimpleMultiMap multiMap) {
		setNamespacePrefixes(multiMap.getMap(NAMESPACES_CONF));
		setHomePrefix(multiMap.getMap(NAMESPACES_CONF).get(""));
	}

	public void configure(final Properties properties) {
		final Map<String, String> map = new HashMap<String, String>();
		for (Entry<Object, Object> entry : properties.entrySet()) {
			map.put(entry.getKey().toString(), entry.getValue().toString());
		}
		setNamespacePrefixes(map);
		setHomePrefix(properties.get("").toString());
	}

	@Override
	public <R extends ObjectReceiver<Model>> R setReceiver(final R receiver) {
		this.receiver = receiver;
		return receiver;
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
	public void process(final Reader reader) {
		// TODO: Handle different RDF serializations
		model.read(reader, homePrefix);  // TODO: Check, whether it is correct to use home prefix here		
	}

	@Override
	public void reset() {
		model.removeAll();
		if (null != receiver) {
			receiver.reset();
		}
	}

	@Override
	public void closeResources() {
		if (null != receiver) {
			if (!model.isEmpty()) {
				receiver.process(model);
			}
			receiver.closeResources();
		}
		model.close();
	}

	private void addProperty(final Resource resource, final String name, final String value) {
		if (value.startsWith(HTTP)) {
			resource.addProperty(createProperty(name), model.createResource(value));
		} else {
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

	@Override
	public void batchComplete(final RecordBatcher modelBatcher) {
		receiver.process(model);
		model.removeAll();

	}

}
