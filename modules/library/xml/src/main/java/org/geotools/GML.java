package org.geotools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.namespace.QName;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import net.opengis.wfs.FeatureCollectionType;
import net.opengis.wfs.WfsFactory;

import org.eclipse.xsd.XSDComplexTypeDefinition;
import org.eclipse.xsd.XSDCompositor;
import org.eclipse.xsd.XSDDerivationMethod;
import org.eclipse.xsd.XSDElementDeclaration;
import org.eclipse.xsd.XSDFactory;
import org.eclipse.xsd.XSDForm;
import org.eclipse.xsd.XSDImport;
import org.eclipse.xsd.XSDModelGroup;
import org.eclipse.xsd.XSDParticle;
import org.eclipse.xsd.XSDSchema;
import org.eclipse.xsd.XSDTypeDefinition;
import org.eclipse.xsd.util.XSDConstants;
import org.eclipse.xsd.util.XSDResourceImpl;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.SchemaImpl;
import org.geotools.gml.producer.FeatureTransformer;
import org.geotools.gtxml.GTXML;
import org.geotools.referencing.CRS;
import org.geotools.xml.Configuration;
import org.geotools.xml.Encoder;
import org.geotools.xml.Parser;
import org.geotools.xml.StreamingParser;
import org.geotools.xml.XSD;
import org.geotools.xs.XS;
import org.geotools.xs.XSConfiguration;
import org.geotools.xs.XSSchema;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.ComplexType;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.Name;
import org.opengis.feature.type.PropertyDescriptor;
import org.opengis.feature.type.Schema;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.xml.sax.SAXException;

import com.vividsolutions.jts.geom.Geometry;

/**
 * UtilityClass for encoding GML content.
 * <p>
 * This utility class uses a range of GeoTools technologies as required; if you would like finer
 * grain control over the encoding process please review the source code of this class and take your
 * own measures.
 * <p>
 *
 *
 * @source $URL$
 */
public class GML
{	/** Version of encoder to use */
	public static abstract class Version {
		public final static GMLVersion GML2 = new GML2Version();

		public final static GMLVersion GML3 = new GML3Version();

		public final static GMLVersion GML3_2 = new GML3_2Version();

		public final static WFSVersion WFS1_0 = new WFS1_0Version();

		public final static WFSVersion WFS1_1 = new WFS1_1Version();

		public final static WFSVersion WFS2_0 = new WFS2_0Version();

		public final static Version[] ALL = new Version[] { GML2, GML3, GML3_2, WFS1_0, WFS1_1, WFS2_0 };

		final String versionStr;

		Configuration configuration = null;

		public Version(String versionStr) {
			this.versionStr = versionStr;
		}

		public synchronized Configuration getConfiguration() {
			if (this.configuration == null) {
				this.configuration = createConfiguration();
			}
			return this.configuration;
		}

		public String getVersionStr() {
			return versionStr;
		}

		public abstract String getNamespace();

		public abstract String getSchemaLocation();

		abstract Configuration createConfiguration();

		public abstract Schema getSchema();

		public abstract String getSubstitutionGroup();

		public static Version valueOf(String str) {
			for (Version version : ALL) {
				// TODO allow substrings
				if (version.versionStr.equals(str)) {
					return version;
				}
			}
			throw new IllegalArgumentException("No version for string '" + str + "' found!");
		}
	}

	/**
	 * Basic abstract class for gml versions
	 */
	public static abstract class GMLVersion extends Version
	{
		Schema schema = null;

		final String namespaceUri;

		final String schemaLocation;

		final String substitutionGroup;

		public GMLVersion(String versionStr, String namespace, String schemaLocation,
			String substitutionGroup) {
			super(versionStr);
			this.namespaceUri = namespace;
			this.schemaLocation = schemaLocation;
			this.substitutionGroup = substitutionGroup;
		}

		@Override
		public String getNamespace() {
			return this.namespaceUri;
		}

		@Override
		public String getSchemaLocation() {
			return this.schemaLocation;
		}

		@Override
		public String getSubstitutionGroup() {
			return substitutionGroup;
		}

		@Override
		public synchronized Schema getSchema() {
			if (this.schema == null)
				this.schema = createSchema();
			return this.schema;
		}

		public abstract Schema createSchema();
	}

	/**
	* Basic abstract class for wfs versions
	 */
	public static abstract class WFSVersion extends Version {
		final GMLVersion gml;

		public WFSVersion(String versionStr, GMLVersion gml) {
			super(versionStr);
			this.gml = gml;
		}

		@Override
		public String getNamespace() {
			return gml.getNamespace();
		}

		@Override
		public String getSchemaLocation() {
			return gml.getSchemaLocation();
		}

		@Override
		public Schema getSchema() {
			return gml.getSchema();
		}

		@Override
		public String getSubstitutionGroup() {
			return gml.getSubstitutionGroup();
		}
	}

	static class GML2Version extends GMLVersion {

		public GML2Version() {
			super("2.1.2", org.geotools.gml2.GML.NAMESPACE, "gml/2.1.2/feature.xsd", "_Feature");
		}

		@Override
		Configuration createConfiguration() {
			return new org.geotools.gml2.GMLConfiguration();
		}

		@Override
		public Schema createSchema() {
			return new org.geotools.gml2.GMLSchema().profile();
		}
	}

	static class GML3Version extends GMLVersion {

		public GML3Version() {
			super("3.1.1", org.geotools.gml3.GML.NAMESPACE, "gml/3.1.1/base/gml.xsd", "_Feature");
		}

		@Override
		Configuration createConfiguration() {
			return new org.geotools.gml3.GMLConfiguration();
		}

		@Override
		public Schema createSchema() {
			return new org.geotools.gml3.GMLSchema().profile();
		}
	}

	static class GML3_2Version extends GMLVersion {

		public GML3_2Version() {
			super("3.2.1", org.geotools.gml3.v3_2.GML.NAMESPACE, "gml/3.2.1/base/gml.xsd",
				"AbstractFeature");
		}

		@Override
		Configuration createConfiguration() {
			return new org.geotools.gml3.v3_2.GMLConfiguration();
		}

		@Override
		public Schema createSchema() {
			return org.geotools.gml3.v3_2.GML.getInstance().getTypeMappingProfile();
		}
	}

	static class WFS1_0Version extends WFSVersion {
		
		public WFS1_0Version() {
			super("1.0.0", GML2);
		}

		@Override
		Configuration createConfiguration() {
			return new org.geotools.wfs.v1_0.WFSConfiguration();
		}
	}

	static class WFS1_1Version extends WFSVersion {
		
		public WFS1_1Version() {
			super("1.1.0", GML3);
		}

		@Override
		Configuration createConfiguration() {
			return new org.geotools.wfs.v1_1.WFSConfiguration();
		}
	}

	static class WFS2_0Version extends WFSVersion {
		
		public WFS2_0Version() {
			super("2.0.0", GML3_2);
		}

		@Override
		Configuration createConfiguration() {
			return new org.geotools.wfs.v2_0.WFSConfiguration();
		}
	}

	protected Charset encoding = Charset.forName("UTF-8");

	protected URL baseURL;

	/** Schema or profile used to map between Java classes and XML elements. */
	protected List<Schema> schemaList = new ArrayList<Schema>();

	/** The prefix of the default namespace */
	private String namespacePrefix = null;

	/** Add this namespace prefixes to the xml */
	protected Map<String, String> namespaces = new HashMap<String, String>();

	private boolean indenting = false;

	protected final Version version;

	private boolean legacy;

	private CoordinateReferenceSystem crs;

	/**
	 * Construct a GML utility class to work with the indicated version of GML.
	 * <p>
	 * Note that when working with GML2 you need to supply additional information prior to use (in
	 * order to indicate where for XSD file is located).
	 * 
	 * @param version
	 *            Version of GML to use
	 */
	public GML(Version version) {
		this.version = version;
		init();
	}


	/**
	 * Engage legacy support for GML2.
	 * <p>
	 * The GML2 support for FeatureTransformer is much faster then that provided by the GTXML
	 * parser/encoder. This speed is at the expense of getting the up front configuration exactly
	 * correct (something you can only tell when parsing the produced result!). Setting this value
	 * to false will use the same GMLConfiguration employed when parsing and has less risk of
	 * producing invalid content.
	 * 
	 * @param legacy
	 */
	public void setLegacy(boolean legacy) {
		this.legacy = legacy;
	}

	/**
	 * Set the target namespace for the encoding.
	 * 
	 * @param prefix
	 * @param namespace
	 */
	public void setNamespace(String prefix, String namespace) {
		addNamespace(prefix, namespace);
		this.namespacePrefix = prefix;
	}

	public String getNamespaceURI() {
		return this.namespaces.get(this.namespacePrefix);
	}

	public void addNamespace(String prefix, String namespaceUri) {
		this.namespaces.put(prefix, namespaceUri);
	}

	/**
	 * Set the encoding to use.
	 * 
	 * @param encoding
	 */
	public void setEncoding(Charset encoding) {
		this.encoding = encoding;
	}

	/**
	 * Base URL to use when encoding
	 */
	public void setBaseURL(URL baseURL) {
		this.baseURL = baseURL;
	}

	/**
	 * Coordinate reference system to use when decoding.
	 * <p>
	 * In a few cases (such as decoding a SimpleFeatureType) the file format does not include the
	 * required CooridinateReferenceSystem and you are asked to supply it.
	 * 
	 * @param crs
	 */
	public void setCoordinateReferenceSystem(CoordinateReferenceSystem crs) {
		this.crs = crs;
	}

	/**
	* 
	* @param indenting
	*/
	public void setIndenting(boolean indenting) {
		this.indenting = indenting;
	}

	/**
	 * 
	 * @return
	 */
	public boolean getIndenting() {
		return this.indenting;
	}

	/**
	   * Set up out of the box configuration for GML encoding.
	   * <ul>
	   * <li>GML2</li>
	   * <li>GML3</li>
	   * </ul>
	   * The following are not avialable yet:
	   * <ul>
	   * <li>gml3.2 - not yet available</li>
	   * <li>wfs1.0 - not yet available</li>
	   * <li>wfs1.1 - not yet available</li>
	   * </ul>
	   * 
	   * @param version
	   */
	protected void init() {
		schemaList.add(new XSSchema().profile()); // encoding of common java types
		Schema hack = new SchemaImpl(XS.NAMESPACE);

		AttributeTypeBuilder builder = new AttributeTypeBuilder();
		builder.setName("date");
		builder.setBinding(Date.class);
		hack.put(new NameImpl(XS.DATETIME), builder.buildType());

		schemaList.add(hack);

		schemaList.add(version.getSchema());
		namespaces.put("gml", version.getNamespace());
	}

	private Entry<Name, AttributeType> searchSchemas(Class<?> binding) {
		// sort by isAssignable so we get the most specific match possible
		//
		Comparator<Entry<Name, AttributeType>> sort = new Comparator<Entry<Name, AttributeType>>() {
			public int compare(Entry<Name, AttributeType> o1, Entry<Name, AttributeType> o2) {
				Class<?> binding1 = o1.getValue().getBinding();
				Class<?> binding2 = o2.getValue().getBinding();
				if (binding1.equals(binding2)) {
					return 0;
				}
				if (binding1.isAssignableFrom(binding2)) {
					return 1;
				} else {
					return 0;
				}
			}
		};
		List<Entry<Name, AttributeType>> match = new ArrayList<Entry<Name, AttributeType>>();

		// process the listed profiles recording all available matches
		for (Schema profile : schemaList) {
			for (Entry<Name, AttributeType> entry : profile.entrySet()) {
				AttributeType type = entry.getValue();
				if (type.getBinding().isAssignableFrom(binding)) {
					match.add(entry);
				}
			}
		}
		Collections.sort(match, sort);

		Iterator<Entry<Name, AttributeType>> iter = match.iterator();
		if (iter.hasNext()) {
			Entry<Name, AttributeType> entry = iter.next();
			return entry;
		} else {
			return null; // no binding found that matches
		}
	}

	@SuppressWarnings("unchecked")
	public void encode(OutputStream out, SimpleFeatureCollection collection) throws IOException {

		if (version == Version.GML2) {
			if (legacy) {
				encodeLegacyGML2(out, collection);
			} else {
				throw new IllegalStateException("Cannot encode a feature collection using GML2 (only WFS)");
			}
		} else {
			Encoder e = new Encoder(version.getConfiguration());
			for (Map.Entry<String, String> entry : this.namespaces.entrySet()) {
				e.getNamespaces().declarePrefix(entry.getKey(), entry.getValue());
			}
			e.setIndenting(getIndenting());

			FeatureCollectionType featureCollectionType = WfsFactory.eINSTANCE
				.createFeatureCollectionType();
			featureCollectionType.getFeature().add(collection);

			e.encode(featureCollectionType, org.geotools.wfs.WFS.FeatureCollection, out);
		}
	}

	private void encodeLegacyGML2(OutputStream out, SimpleFeatureCollection collection)
		throws IOException {
		final SimpleFeatureType TYPE = collection.getSchema();

		FeatureTransformer transform = new FeatureTransformer();
		transform.setIndentation(4);
		transform.setGmlPrefixing(true);

		if (namespacePrefix != null) {
			String namespaceUri = namespaces.get(namespacePrefix);
			transform.getFeatureTypeNamespaces().declareDefaultNamespace(namespacePrefix, namespaceUri);
			transform.addSchemaLocation(namespacePrefix, namespaceUri);
			// transform.getFeatureTypeNamespaces().declareDefaultNamespace("", namespace );
		}

		if (TYPE.getName().getNamespaceURI() != null && TYPE.getUserData().get("prefix") != null) {
			String typeNamespace = TYPE.getName().getNamespaceURI();
			String typePrefix = (String) TYPE.getUserData().get("prefix");

			transform.getFeatureTypeNamespaces().declareNamespace(TYPE, typePrefix, typeNamespace);
		} else if (namespacePrefix != null) {
			// ignore namespace URI in feature type
			String namespaceUri = namespaces.get(namespacePrefix);
			transform.getFeatureTypeNamespaces().declareNamespace(TYPE, namespacePrefix, namespaceUri);
		} else {
			// hopefully that works out for you then
		}

		// we probably need to do a wfs feaure collection here?
		transform.setCollectionPrefix(null);
		transform.setCollectionNamespace(null);

		// other configuration
		transform.setCollectionBounding(true);
		transform.setEncoding(encoding);

		// configure additional feature namespace lookup
		transform.getFeatureNamespaces();

		String srsName = CRS.toSRS(TYPE.getCoordinateReferenceSystem());
		if (srsName != null) {
			transform.setSrsName(srsName);
		}

		try {
			transform.transform(collection, out);
		} catch (TransformerException e) {
			throw (IOException) new IOException("Failed to encode feature collection:" + e)
				.initCause(e);
		}
	}

	/**
	 * Encode the provided SimpleFeatureType into an XSD file, using a target namespace
	 * <p>
	 * When encoding the simpleFeatureType:
	 * <ul>
	 * <li>target prefix/namespace can be provided by prefix and namespace parameters. This is the
	 * default for the entire XSD document.</li>
	 * <li>simpleFeatureType.geName().getNamespaceURI() is used for the simpleFeatureType itself,
	 * providing simpleFeatureType.getUserData().get("prefix") is defined</li>
	 * </ul>
	 * 
	 * @param simpleFeatureType
	 *            To be encoded as an XSD document
	 * @param prefix
	 *            Prefix to use for for target namespace
	 * @param namespace
	 *            Target namespace
	 */
	public void encode(OutputStream out, SimpleFeatureType simpleFeatureType) throws IOException {
		XSDSchema xsd = xsd(simpleFeatureType);

		XSDResourceImpl.serialize(out, xsd.getElement(), encoding.name());
	}

	/**
	 * Decode a typeName from the provided schemaLocation.
	 * <p>
	 * The XMLSchema does not include CoordinateReferenceSystem we need to ask you to supply this
	 * information.
	 * 
	 * @param schemaLocation
	 * @param typeName
	 * @return SimpleFeatureType
	 * @throws IOException
	 */
	public SimpleFeatureType decodeSimpleFeatureType(URL schemaLocation, Name typeName)
		throws IOException {
		if (Version.WFS1_1 == version) {
			final QName featureName = new QName(typeName.getNamespaceURI(), typeName.getLocalPart());

			String namespaceURI = featureName.getNamespaceURI();
			String uri = schemaLocation.toExternalForm();
			Configuration wfsConfiguration = new org.geotools.gml3.ApplicationSchemaConfiguration(
				namespaceURI, uri);

			FeatureType parsed = GTXML.parseFeatureType(wfsConfiguration, featureName, crs);
			return DataUtilities.simple(parsed);
		}

		if (Version.WFS1_0 == version) {
			final QName featureName = new QName(typeName.getNamespaceURI(), typeName.getLocalPart());

			String namespaceURI = featureName.getNamespaceURI();
			String uri = schemaLocation.toExternalForm();

			XSD xsd = new org.geotools.gml2.ApplicationSchemaXSD(namespaceURI, uri);
			Configuration configuration = new Configuration(xsd) {
				{
					addDependency(new XSConfiguration());
					addDependency(version.getConfiguration()); // use our GML configuration
				}

				@SuppressWarnings("rawtypes")
				protected void registerBindings(java.util.Map bindings) {
					// we have no special bindings
				}
			};

			FeatureType parsed = GTXML.parseFeatureType(configuration, featureName, crs);
			return DataUtilities.simple(parsed);
		}
		return null;
	}

	public SimpleFeatureCollection decodeFeatureCollection(InputStream in) throws IOException,
		SAXException, ParserConfigurationException {
		if (Version.GML2 == version || Version.WFS1_0 == version || Version.GML2 == version
			|| Version.GML3 == version || Version.WFS1_0 == version
			|| Version.WFS1_1 == version) {
			Parser parser = new Parser(version.getConfiguration());
			Object obj = parser.parse(in);
			SimpleFeatureCollection collection = toFeatureCollection(obj);
			return collection;
		}
		return null;
	}

	/**
	 * Convert parse results into a SimpleFeatureCollection.
	 * 
	 * @param obj SimpleFeatureCollection, Collection<?>, SimpleFeature, etc...
	 * @return SimpleFeatureCollection of the results
	 */
	private SimpleFeatureCollection toFeatureCollection(Object obj) {
		if (obj == null) {
			return null; // not available?
		}
		if (obj instanceof SimpleFeatureCollection) {
			return (SimpleFeatureCollection) obj;
		}
		if (obj instanceof Collection<?>) {
			Collection<?> collection = (Collection<?>) obj;
			SimpleFeatureCollection simpleFeatureCollection = simpleFeatureCollection(collection);
			return simpleFeatureCollection;
		}
		if (obj instanceof SimpleFeature) {
			SimpleFeature feature = (SimpleFeature) obj;
			return DataUtilities.collection(feature);
		}
		if (obj instanceof FeatureCollectionType) {
			FeatureCollectionType collectionType = (FeatureCollectionType) obj;
			for (Object entry : collectionType.getFeature()) {
				SimpleFeatureCollection collection = toFeatureCollection(entry);
				if (entry != null) {
					return collection;
				}
			}
			return null; // nothing found
		} else {
			throw new ClassCastException(obj.getClass() 
				+ " produced when FeatureCollection expected"
				+ " check schema use of AbstractFeatureCollection");
		}
	}

	/**
	 * Allow the parsing of features as a stream; the returned iterator can be used to step through
	 * the inputstream of content one feature at a time without loading everything into memory.
	 * <p>
	 * The schema used by the XML is consulted to determine what element extends AbstractFeature.
	 * 
	 * @param in
	 * @return Iterator that can be used to parse features one at a time
	 * @throws SAXException
	 * @throws ParserConfigurationException
	 * @throws IOException
	 */
	public SimpleFeatureIterator decodeFeatureIterator(InputStream in) throws IOException,
		ParserConfigurationException, SAXException {
		return decodeFeatureIterator(in, null);
	}

	/**
	 * Allow the parsing of features as a stream; the returned iterator can be used to step through
	 * the inputstream of content one feature at a time without loading everything into memory.
	 * <p>
	 * The use of an elementName is optional; and can be used as a workaround in cases where the
	 * schema is not available or correctly defined. The returned elements are wrapped up as a
	 * Feature if needed. This mehtod can be used to retrive only the Geometry elements from a GML
	 * docuemnt.
	 * 
	 * @param in
	 *            InputStream used as a source of SimpleFeature content
	 * @param xpath
	 *            Optional xpath used to indicate simple feature element; the schema will be checked
	 *            for an entry that extends AbstratFeatureType
	 * @return
	 * @throws SAXException
	 * @throws ParserConfigurationException
	 */
	public SimpleFeatureIterator decodeFeatureIterator(InputStream in, QName elementName)
		throws IOException, ParserConfigurationException, SAXException {
		if (Version.GML2 == version || Version.GML3 == version || Version.WFS1_0 == version
			|| Version.WFS1_1 == version) {
			// ParserDelegate parserDelegate = new XSDParserDelegate( gmlConfiguration );
			StreamingParser parser;
			if (elementName != null) {
				parser = new StreamingParser(version.getConfiguration(), in, elementName);
			}
			else {
				parser = new StreamingParser(version.getConfiguration(), in, SimpleFeature.class);
			}
			return iterator(parser);
		}
		return null;
	}

	/**
	 * Go through collection contents and morph contents into SimpleFeatures as required.
	 * 
	 * @param collection
	 * @return SimpleFeatureCollection
	 */
	private SimpleFeatureCollection simpleFeatureCollection(Collection<?> collection) {
    DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
		SimpleFeatureType schema = null;
		for (Object obj : collection) {
			if (schema == null) {
				schema = simpleType(obj);
			}
			SimpleFeature feature = simpleFeature(obj, schema);
			featureCollection.add(feature);
		}
		return featureCollection;
	}

	/**
	 * Used to wrap up a StreamingParser as a Iterator<SimpleFeature>.
	 * <p>
	 * This iterator is actually forgiving; and willing to "morph" content into a SimpleFeature if
	 * needed.
	 * <ul>
	 * <li>SimpleFeature - is returned as is
	 * <li>
	 * 
	 * @param parser
	 * @return
	 */
	protected SimpleFeatureIterator iterator(final StreamingParser parser) {
		return new SimpleFeatureIterator() {
			SimpleFeatureType schema;

			Object next;

			public boolean hasNext() {
				if (next != null) {
					return true;
				}
				next = parser.parse();
				return next != null;
			}

			public SimpleFeature next() {
				if (next == null) 	{
					next = parser.parse();
				}
				if (next != null) {
					try {
						if (schema == null) {
							schema = simpleType(next);
						}
						SimpleFeature feature = simpleFeature(next, schema);
						return feature;
					} finally {
						next = null; // we have tried processing this one now
					}
				} else {
					return null; // nothing left
				}
			}

			public void close() {
				schema = null;
			}
		};
	}

	protected SimpleFeatureType simpleType(Object obj) {
		if (obj instanceof SimpleFeature) 	{
			SimpleFeature feature = (SimpleFeature) obj;
			return feature.getFeatureType();
		}
		if (obj instanceof Map<?, ?>) {
			Map<?, ?> map = (Map<?, ?>) obj;
			SimpleFeatureTypeBuilder build = new SimpleFeatureTypeBuilder();
			build.setName("Unknown");
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				String key = (String) entry.getKey();
				Object value = entry.getValue();
				Class<?> binding = value == null ? Object.class : value.getClass();
				if (value instanceof Geometry) {
					Geometry geom = (Geometry) value;
					Object srs = geom.getUserData();
					if (srs instanceof CoordinateReferenceSystem) {
						build.add(key, binding, (CoordinateReferenceSystem) srs);
					} else if (srs instanceof Integer) {
						build.add(key, binding, (Integer) srs);
					} else if (srs instanceof String) {
						build.add(key, binding, (String) srs);
					} else {
						build.add(key, binding);
					}
				} else {
					build.add(key, binding);
				}
			}
			SimpleFeatureType schema = build.buildFeatureType();
			return schema;
		}
		if (obj instanceof Geometry) {
			Geometry geom = (Geometry) obj;
			Class<?> binding = geom.getClass();
			Object srs = geom.getUserData();

			SimpleFeatureTypeBuilder build = new SimpleFeatureTypeBuilder();
			build.setName("Unknown");
			if (srs instanceof CoordinateReferenceSystem) {
				build.add("the_geom", binding, (CoordinateReferenceSystem) srs);
			} else if (srs instanceof Integer) {
				build.add("the_geom", binding, (Integer) srs);
			} else if (srs instanceof String) {
				build.add("the_geom", binding, (String) srs);
			} else {
				build.add("the_geom", binding);
			}
			build.setDefaultGeometry("the_geom");
			SimpleFeatureType schema = build.buildFeatureType();
			return schema;
		}
		return null;
	}

	/**
	 * Morph provided obj to a SimpleFeature if possible.
	 * 
	 * @param obj
	 * @param schema
	 * @return SimpleFeature, or null if not possible
	 */
	protected SimpleFeature simpleFeature(Object obj, SimpleFeatureType schema) {
		if (schema == null) {
			schema = simpleType(obj);
		}

		if (obj instanceof SimpleFeature) {
			return (SimpleFeature) obj;
		}
		if (obj instanceof Map<?, ?>) {
			Map<?, ?> map = (Map<?, ?>) obj;
			Object values[] = new Object[schema.getAttributeCount()];
			for (int i = 0; i < schema.getAttributeCount(); i++) {
				AttributeDescriptor descriptor = schema.getDescriptor(i);
				String key = descriptor.getLocalName();
				Object value = map.get(key);

				values[i] = value;
			}
			SimpleFeature feature = SimpleFeatureBuilder.build(schema, values, null);
			return feature;
		}
		if (obj instanceof Geometry) {
			Geometry geom = (Geometry) obj;
			SimpleFeatureBuilder build = new SimpleFeatureBuilder(schema);
			build.set(schema.getGeometryDescriptor().getName(), geom);

			SimpleFeature feature = build.buildFeature(null);
			return feature;
		}
		return null; // not available as a feature!
	}
	
	public XSDSchema xsd(SimpleFeatureType... types) throws IOException {
		XSDFactory factory = XSDFactory.eINSTANCE;
		XSDSchema xsd = factory.createXSDSchema();

		xsd.setSchemaForSchemaQNamePrefix("xsd");
		xsd.getQNamePrefixToNamespaceMap().put("xsd", XSDConstants.SCHEMA_FOR_SCHEMA_URI_2001);
		xsd.setElementFormDefault(XSDForm.get(XSDForm.QUALIFIED));

		if (baseURL == null) {
			throw new IllegalStateException("Please setBaseURL prior to encoding");
		}

		for (Map.Entry<String, String> namespace : this.namespaces.entrySet()) {
			xsd.getQNamePrefixToNamespaceMap().put(namespace.getKey(), namespace.getValue());
		}

		if (namespacePrefix != null) {
			String namespaceUri = namespaces.get(namespacePrefix);
			xsd.setTargetNamespace(namespaceUri);
		} else {
			xsd.setTargetNamespace(types[0].getName().getNamespaceURI());
		}

		for (SimpleFeatureType type : types) {
			if (type.getUserData().get("schemaURI") != null) {
				throw new IllegalArgumentException("Unable to support app-schema supplied types");
			}

			if (type.getName().getNamespaceURI() != null && type.getUserData().get("prefix") != null) {
				String providedNamespace = type.getName().getNamespaceURI();
				String providedPrefix = (String) type.getUserData().get("prefix");
				xsd.getQNamePrefixToNamespaceMap().put(providedPrefix, providedNamespace);
			}
		}

		// import GML import
		XSDImport gml = factory.createXSDImport();
		gml.setNamespace(version.getNamespace());
		gml.setSchemaLocation(baseURL.toString() + "/" + version.getSchemaLocation());
		gml.setResolvedSchema(version.getConfiguration().getXSD().getSchema());
		xsd.getContents().add(gml);

		// TODO make AbstractFeatureType dynamic
		XSDComplexTypeDefinition abstractComplexType = xsd.resolveComplexTypeDefinition(version
			.getNamespace(), "AbstractFeatureType");
		XSDElementDeclaration substitutionGroup = xsd.resolveElementDeclaration(version.getNamespace(),
			version.getSubstitutionGroup());

		// create element nodes
		for (SimpleFeatureType type : types) {
			XSDComplexTypeDefinition complexType = xsd(xsd, type, abstractComplexType);

			XSDElementDeclaration element = factory.createXSDElementDeclaration();
			element.setName(type.getTypeName());
			element.setTargetNamespace(type.getName().getNamespaceURI());
			element.setSubstitutionGroupAffiliation(substitutionGroup);
			element.setTypeDefinition(complexType);
			xsd.getContents().add(element);
		}

		xsd.updateElement();
		return xsd;
	}

	/**
	 * Build the XSD definition for the provided type.
	 * <p>
	 * The generated definition is recorded in the XSDSchema prior to being returned.
	 * 
	 * @param xsd
	 *            The XSDSchema being worked on
	 * @param type
	 *            ComplexType to capture as an encoding, usually a SimpleFeatureType
	 * @param L_TYPE
	 *            definition to use as the base type, or null
	 * @return XSDComplexTypeDefinition generated for the provided type
	 */
	protected XSDComplexTypeDefinition xsd(XSDSchema xsd, ComplexType type,
		final XSDComplexTypeDefinition BASE_TYPE) {
		XSDFactory factory = XSDFactory.eINSTANCE;

		XSDComplexTypeDefinition definition = factory.createXSDComplexTypeDefinition();
		definition.setName(type.getName().getLocalPart() + "Type");
		definition.setDerivationMethod(XSDDerivationMethod.EXTENSION_LITERAL);

		if (BASE_TYPE != null) {
			definition.setBaseTypeDefinition(BASE_TYPE);
		}
		List<String> skip = Collections.emptyList();
		if ("AbstractFeatureType".equals(BASE_TYPE.getName())) {
			// should look at ABSTRACT_FEATURE_TYPE to determine contents to skip
			skip = Arrays.asList(new String[] { "nounds", "description", "boundedBy" });
		}

		// attributes
		XSDModelGroup attributes = factory.createXSDModelGroup();
		attributes.setCompositor(XSDCompositor.SEQUENCE_LITERAL);

		Name anyName = new NameImpl(XS.NAMESPACE, XS.ANYTYPE.getLocalPart());

		for (PropertyDescriptor descriptor : type.getDescriptors()) {
			
			if (descriptor instanceof AttributeDescriptor) {
				AttributeDescriptor attributeDescriptor = (AttributeDescriptor) descriptor;

				if (skip.contains(attributeDescriptor.getLocalName())) {
					continue;
				}

				XSDElementDeclaration attribute = factory.createXSDElementDeclaration();
				attribute.setName(attributeDescriptor.getLocalName());
				attribute.setNillable(attributeDescriptor.isNillable());

				Name name = attributeDescriptor.getType().getName();

				// return the first match.
				if (!anyName.equals(name)) {
					AttributeType attributeType = attributeDescriptor.getType();

					if (attributeType instanceof ComplexType) {
						ComplexType complexType = (ComplexType) attributeType;
						// any complex contents must resolve (we cannot encode against
						// an abstract type for example)
						if (xsd.resolveTypeDefinition(name.getNamespaceURI(), name.getLocalPart()) == null) {
							// not yet added; better add it into the mix
							xsd(xsd, complexType, null);
						}
					} else {
						Class<?> binding = attributeType.getBinding();
						Entry<Name, AttributeType> entry = searchSchemas(binding);
						if (entry == null) {
							throw new IllegalStateException("No type for " + attribute.getName()
								+ " (" + binding.getName() + ")");
						}
						name = entry.getKey();
					}
				}

				XSDTypeDefinition attributeDefinition = xsd.resolveTypeDefinition(name
						.getNamespaceURI(), name.getLocalPart());
				attribute.setTypeDefinition(attributeDefinition);

				XSDParticle particle = factory.createXSDParticle();
				particle.setMinOccurs(attributeDescriptor.getMinOccurs());
				particle.setMaxOccurs(attributeDescriptor.getMaxOccurs());
				particle.setContent(attribute);
				attributes.getContents().add(particle);
			}
		}

		// set up fatureType with attributes
		XSDParticle contents = factory.createXSDParticle();
		contents.setContent(attributes);

		definition.setContent(contents);
		xsd.getContents().add(definition);

		return definition;
	}
}