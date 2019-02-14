/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.rtempleton.NifiSnowpipe;

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
import org.apache.nifi.processor.util.StandardValidators;

import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.IngestResponse;
import net.snowflake.ingest.utils.StagedFileWrapper;

@Tags({"Snowflake, Snowpipe"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Used to invoke Snowflake Snowpipe call to asynchronously process staged file")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="snowpipe.ingest.reponsecode", description="HTTP status code returned from Snowpipe invocation")})
public class SnowpipeIngest extends AbstractProcessor {
	
	private static final String schemeName = "https";
	private static final int portNum = 443;

	//properties
	public static final PropertyDescriptor REGION = new PropertyDescriptor
			.Builder().name("Region")
			.displayName("Snowflake Region")
			.description("Snowflake Region")
			.allowableValues(new AllowableValue("snowflakecomputing.com", "AWS US West"),
							new AllowableValue("us-east-1.snowflakecomputing.com", "AWS US East"),
							new AllowableValue("eu-central-1.snowflakecomputing.com", "AWS EU (Frankfurt)"),
							new AllowableValue("eu-west-1.snowflakecomputing.com", "AWS EU (Dublin)"),
							new AllowableValue("ap-southeast-2.snowflakecomputing.com", "AWS Asia Pacific (Sydney)"),
							new AllowableValue("east-us-2.azure.snowflakecomputing.com", "Azure East US 2"),
							new AllowableValue("west-europe.azure.snowflakecomputing.com", "Azure West Europe"))
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor ACCOUNT = new PropertyDescriptor
			.Builder().name("Account")
			.displayName("Snowflake account name")
			.description("Snowflake account name")
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

	public static final PropertyDescriptor FILE = new PropertyDescriptor
			.Builder().name("File")
			.displayName("File Name")
			.description("Name of file to be processed by the given Pipe")
			.required(true)
			.expressionLanguageSupported(true)
//			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.defaultValue("${filename}")
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

	//attributes
	private static final String RESPONSE_ATT = "snowpipe.ingest.reponsecode";

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(REGION);
		descriptors.add(ACCOUNT);
		descriptors.add(USER);
		descriptors.add(PIPE);
		descriptors.add(PRIVATEKEY);
		descriptors.add(FILE);
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
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if ( flowFile == null ) {
			return;
		}

		final ComponentLog logger = getLogger();
		final String host = context.getProperty(REGION).getValue();
		final String account = context.getProperty(ACCOUNT).getValue();
		final String user = context.getProperty(USER).toString();
		final String pipe = context.getProperty(PIPE).evaluateAttributeExpressions(flowFile).getValue();
		final String pk = context.getProperty(PRIVATEKEY).getValue();
		final String file = context.getProperty(FILE).evaluateAttributeExpressions(flowFile).getValue();


		try {
			byte[] encoded = Base64.decodeBase64(pk);
			KeyFactory kf = KeyFactory.getInstance("RSA");

			EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
			final PrivateKey privateKey = kf.generatePrivate(keySpec);


			SimpleIngestManager mgr = new SimpleIngestManager(account, user, pipe, privateKey, schemeName, host, portNum);
			IngestResponse resp = mgr.ingestFile(new StagedFileWrapper(file), null);
			flowFile = session.putAttribute(flowFile, RESPONSE_ATT, resp.getResponseCode());
			session.transfer(flowFile, REL_SUCCESS);

		}catch(Exception e) {
			logger.error("Unable to call pipe {} on file {} due to {}; routing to failure",
                    new Object[]{pipe, file, e});
			session.transfer(flowFile, REL_FAILURE);

		}
	}
}
