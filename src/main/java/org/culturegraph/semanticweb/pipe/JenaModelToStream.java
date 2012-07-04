package org.culturegraph.semanticweb.pipe;

import org.culturegraph.metastream.framework.DefaultSender;
import org.culturegraph.metastream.framework.ObjectReceiver;
import org.culturegraph.metastream.framework.StreamReceiver;
import org.culturegraph.util.MakeIterable;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;

public final class JenaModelToStream extends DefaultSender<StreamReceiver>
		implements ObjectReceiver<Model> {

	@Override
	public void process(final Model model) {
		for (Resource resource : MakeIterable.wrap(model.listSubjects())) {
			if (resource.isURIResource()) {
				getReceiver().startRecord(resource.getURI());
				processResource(resource);
				getReceiver().endRecord();
			}
		}
	}
	
	// TODO: Prevent circuits!
	private void processResource(final Resource resource) {
		for (Statement statement : MakeIterable.wrap(resource.listProperties())) {
			final RDFNode obj = statement.getObject();
			if (obj.isLiteral()) {
				getReceiver().literal(statement.getPredicate().getURI(), obj.asLiteral().getString());
			} else if (obj.isURIResource()) {
				getReceiver().literal(statement.getPredicate().getURI(), obj.asResource().getURI());				
			} else if (obj.isAnon()) {
				getReceiver().startEntity(statement.getPredicate().getURI());
				processResource(obj.asResource());
				getReceiver().endEntity();
			}
		}
	}

}
