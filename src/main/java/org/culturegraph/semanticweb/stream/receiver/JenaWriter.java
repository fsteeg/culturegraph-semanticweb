package org.culturegraph.semanticweb.stream.receiver;

import java.util.Map;

import org.culturegraph.metamorph.core.exceptions.MetamorphException;
import org.culturegraph.metamorph.multimap.SimpleMultiMap;
import org.culturegraph.metamorph.stream.StreamReceiver;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Seq;

/**
 * This is unfinished code!
 * 
 * @author Markus Michael Geipel
 *
 */
public final class JenaWriter implements StreamReceiver {
	public static final String NAMESPACES_CONF = "namespaces";
	public static final String LISTELEMENTS_CONF = "listelements";
	public static final int DEFAULT_BATCH_SIZE = 1000;
	
	private static final String HTTP = "http://";
	private final Model model;
	private Resource currentResource;
	private int batchSize = DEFAULT_BATCH_SIZE;
	private long count;
	private BatchFinishedListener batchFinishedListener;
	private Resource blankNode;
	private SimpleMultiMap multiMapProvider;
	private String homePrefix;
	
	public JenaWriter() {
		model = ModelFactory.createDefaultModel();
	}
	
	
	public JenaWriter(final Model model) {
		this.model = model;
	}
	
	public void configure(final SimpleMultiMap multiMapProvider){
		final Map<String, String> namespaces = multiMapProvider.getMap(NAMESPACES_CONF);
		if(namespaces==null){
			throw new MetamorphException("namespaces map is missing");
		}
		model.setNsPrefixes(namespaces);
		homePrefix = namespaces.get("");
		if(homePrefix==null){
			throw new MetamorphException("home namespace is missing. (set \"\" namespace)");
		}
		this.multiMapProvider = multiMapProvider;
	}
	
	public Model getModel(){
		return model;
	}
	
	@Override
	public void startRecord(final String identifier) {
		currentResource = model.createResource(homePrefix + identifier);

	}

	@Override
	public void endRecord() {
		++count;
		if(count%batchSize==0){
			if(null!=batchFinishedListener){
				batchFinishedListener.onBatchFinished(model);
			}
			model.removeAll();
		}
	}

	@Override
	public void startEntity(final String name) {
		if (multiMapProvider.getMap(LISTELEMENTS_CONF).keySet().contains(name)) {
			blankNode = model.createSeq();
			currentResource.addProperty(createProperty(name), blankNode);
		} else {
			blankNode = model.createResource();
			currentResource.addProperty(createProperty(name), blankNode);
		}
	}

	@Override
	public void endEntity() {
		blankNode = null;
	}

	@Override
	public void literal(final String name, final String value) {
		if(value.startsWith(HTTP)){
			/*
			 * value is a reference to a rdf:resource
			 */
			Resource resourceValue = model.createResource(value);
			if(blankNode==null){
				currentResource.addProperty(createProperty(name), resourceValue);
			} else if (blankNode instanceof Seq) {
				((Seq)blankNode).add(resourceValue);
			} else {
				blankNode.addProperty(createProperty(name), resourceValue);
			}
		} else {
			/*
			 * value is a literal
			 */
			if(blankNode==null){
				currentResource.addProperty(createProperty(name), value);
			} else if (blankNode instanceof Seq) {
				((Seq)blankNode).add(value);
			} else {
				blankNode.addProperty(createProperty(name), value);
			}
		}
	}
	
	private Property createProperty(final String name){
		if(name.startsWith(HTTP)){
			return model.createProperty(name);
		}
		
		final int cut = name.indexOf(':');
		if(cut>0){
			final String prefix = multiMapProvider.getValue(NAMESPACES_CONF, name.substring(0, cut));
			if(prefix!=null){
				return model.createProperty(prefix, name.substring(cut+1));
			}
		}
		throw new MetamorphException("'" + name + "' is not a valid URI");
	}

	/**
	 * @param batchSize
	 */
	public void setBatchSize(final int batchSize) {
		this.batchSize = batchSize;
	}

	/**
	 * @return
	 */
	public int getBatchSize() {
		return batchSize;
	}
	
	/**
	 * @param batchFinishedListener
	 */
	public void setBatchFinishedListener(final BatchFinishedListener batchFinishedListener) {
		this.batchFinishedListener = batchFinishedListener;
	}
	
	public interface BatchFinishedListener {
		void onBatchFinished(final Model model);
	}
}
