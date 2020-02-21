package com.github.rtempleton.NifiSnowpipe;

import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.joda.time.DateTime;

import com.google.gson.Gson;

import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.HistoryRangeResponse;
import net.snowflake.ingest.connection.HistoryResponse.FileEntry;



@TriggerWhenEmpty
@Tags({"Snowflake, Snowpipe"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Used to invoke Snowflake Snowpipe call to query loaded files for a given pipe in a time range")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class SnowpipeHistoryRange extends AbstractProcessor {

	private static final String schemeName = "https";
	private static final int portNum = 443;

	//properties
//	public static final PropertyDescriptor REGION = new PropertyDescriptor
//			.Builder().name("Region")
//			.displayName("Snowflake Region")
//			.description("Snowflake Region")
//			.allowableValues(new AllowableValue("snowflakecomputing.com", "AWS US West"),
//					new AllowableValue("us-east-1.snowflakecomputing.com", "AWS US East"),
//					new AllowableValue("ca-central-1.snowflakecomputing.com", "AWS Canada (Central)"),
//					new AllowableValue("eu-central-1.snowflakecomputing.com", "AWS EU (Frankfurt)"),
//					new AllowableValue("eu-west-1.snowflakecomputing.com", "AWS EU (Dublin)"),
//					new AllowableValue("ap-southeast-1.snowflakecomputing.com", "AWS Asia Pacific (Singapore)"),
//					new AllowableValue("ap-southeast-2.snowflakecomputing.com", "AWS Asia Pacific (Sydney)"),
//					new AllowableValue("east-us-2.azure.snowflakecomputing.com", "Azure East US 2"),
//					new AllowableValue("us-gov-virginia.azure.snowflakecomputing.com", "Azure US Gov Virginia"),
//					new AllowableValue("canada-central.azure.snowflakecomputing.com", "Azure Canada Central"),
//					new AllowableValue("west-europe.azure.snowflakecomputing.com", "Azure West Europe"),
//					new AllowableValue("australia-east.azure.snowflakecomputing.com", "Azure Australia East"),
//					new AllowableValue("southeast-asia.azure.snowflakecomputing.com", "Southeast Asia"))
//			.required(true)
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//			.build();
	
	public static final PropertyDescriptor ACCOUNT = new PropertyDescriptor
			.Builder().name("Account")
			.displayName("Fully qualified Snowflake account name")
			.description("<account>.<region>.snowflakecomputing.com")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor USER = new PropertyDescriptor
			.Builder().name("User")
			.displayName("Snowflake user name")
			.description("Snowflake user name")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor PIPE = new PropertyDescriptor
			.Builder().name("Pipe")
			.displayName("Snowflake pipe name")
			.description("Fully qualified Snowflake pipe name")
			.required(true)
			.expressionLanguageSupported(true)
//			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor PRIVATEKEY = new PropertyDescriptor
			.Builder().name("PrivateKey")
			.displayName("Private key")
			.description("Private key used to sign JWT tokens")
			.required(true)
			.sensitive(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor TRAIL = new PropertyDescriptor
			.Builder().name("Trailing Minutes")
			.displayName("Trailing Minutes")
			.description("Number of trailing minutes from the current time to define the time range")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	//relationships
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("Successfully accepted Snowpipe ingest call")
			.build();

	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure")
			.description("Failed Snowpipe ingest calls")
			.build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;



	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
//		descriptors.add(REGION);
		descriptors.add(ACCOUNT);
		descriptors.add(USER);
		descriptors.add(PIPE);
		descriptors.add(PRIVATEKEY);
		descriptors.add(TRAIL);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
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
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		
		
		final ComponentLog logger = getLogger();
//		final String host = context.getProperty(REGION).getValue();
		final String account = context.getProperty(ACCOUNT).getValue();
		final String user = context.getProperty(USER).toString();
		final String pipe = context.getProperty(PIPE).getValue();
		final String pk = context.getProperty(PRIVATEKEY).getValue();
		final int trailMin = context.getProperty(TRAIL).asInteger();
		final byte[] nl = "\n".getBytes();
		
//		FlowFile targetFlowFile = session.create();
		
		try {
			byte[] encoded = Base64.decodeBase64(pk);
			KeyFactory kf = KeyFactory.getInstance("RSA");

			EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
			final PrivateKey privateKey = kf.generatePrivate(keySpec);

			String[] tmp = account.split("\\.");
			SimpleIngestManager mgr = new SimpleIngestManager(tmp[0], user, pipe, privateKey, schemeName, account, portNum);
			DateTime now = new DateTime();
			DateTime then = now.minusMinutes(Math.abs(trailMin));
			HistoryRangeResponse resp = mgr.getHistoryRange(null, then.toString(), now.toString());
			
			if (resp.files.size()>0) {
			
			FlowFile targetFlowFile = session.create();
			targetFlowFile = session.write(targetFlowFile, new OutputStreamCallback() {
				
				@Override
				public void process(OutputStream out) throws IOException {
					Gson gson = new Gson();
//					out.write(gson.toJson(resp.files).getBytes());
					for (FileEntry entry : resp.files) {
						out.write(gson.toJson(entry).getBytes());
						out.write(nl);
					}
					
					
				}
			});

			session.transfer(targetFlowFile, REL_SUCCESS);
			}


		}catch(Exception e) {
			logger.error("Failure invoking historyRange: {}",
                    new Object[]{e});
//			session.transfer(targetFlowFile, REL_FAILURE);


		}
		

	}

}
