
package com.nifi.processor.jsonvalidator.processors.jsonprocessor;

import com.jayway.jsonpath.JsonPath;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class JsonFieldExtractorProcessor extends AbstractProcessor {

    public static final PropertyDescriptor JSON_STRING = new PropertyDescriptor
            .Builder().name("JSON_STRING")
            .displayName("JSON String to validate")
            .description("Json String to validate")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failure relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(JSON_STRING);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final AtomicReference<String> value = new AtomicReference<>();

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final String jField = context.getProperty(JSON_STRING).getValue();

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try {
                    String json = IOUtils.toString(in);

//                    String result = JsonPath.read(json, "$.hello");
                    String result = JsonPath.read(json, jField);
                    value.set(result);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    value.set("");
                    getLogger().error("Failed to read json string.", ex);
                }
            }
        });

        // Write the results to an attribute
        String results = value.get();

        OutputStreamCallback out = new OutputStreamCallback() {
            @Override
            public void process(OutputStream outputStream) throws IOException {
                outputStream.write(value.get().getBytes());
            }
        };

        if (results != null && !results.isEmpty()) {
            flowFile = session.putAttribute(flowFile, "match", results.toString());
            session.write(flowFile, out);
            session.transfer(flowFile, SUCCESS);
        } else {
            flowFile = session.putAttribute(flowFile, "match", "");
            session.write(flowFile, out);
            session.transfer(flowFile, FAILURE);
        }
    }
}
