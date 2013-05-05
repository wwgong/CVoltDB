//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vJAXB 2.1.10 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2011.10.17 at 06:12:41 PM EDT 
//


package org.voltdb.compiler.deploymentfile;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the org.voltdb.compiler.deploymentfile package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

    private final static QName _Deployment_QNAME = new QName("", "deployment");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: org.voltdb.compiler.deploymentfile
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link CommandLogType.Frequency }
     * 
     */
    public CommandLogType.Frequency createCommandLogTypeFrequency() {
        return new CommandLogType.Frequency();
    }

    /**
     * Create an instance of {@link SystemSettingsType }
     * 
     */
    public SystemSettingsType createSystemSettingsType() {
        return new SystemSettingsType();
    }

    /**
     * Create an instance of {@link PathEntry }
     * 
     */
    public PathEntry createPathEntry() {
        return new PathEntry();
    }

    /**
     * Create an instance of {@link HeartbeatType }
     * 
     */
    public HeartbeatType createHeartbeatType() {
        return new HeartbeatType();
    }

    /**
     * Create an instance of {@link SnapshotType }
     * 
     */
    public SnapshotType createSnapshotType() {
        return new SnapshotType();
    }

    /**
     * Create an instance of {@link PartitionDetectionType }
     * 
     */
    public PartitionDetectionType createPartitionDetectionType() {
        return new PartitionDetectionType();
    }

    /**
     * Create an instance of {@link DeploymentType }
     * 
     */
    public DeploymentType createDeploymentType() {
        return new DeploymentType();
    }

    /**
     * Create an instance of {@link UsersType }
     * 
     */
    public UsersType createUsersType() {
        return new UsersType();
    }

    /**
     * Create an instance of {@link ExportType }
     * 
     */
    public ExportType createExportType() {
        return new ExportType();
    }

    /**
     * Create an instance of {@link PartitionDetectionType.Snapshot }
     * 
     */
    public PartitionDetectionType.Snapshot createPartitionDetectionTypeSnapshot() {
        return new PartitionDetectionType.Snapshot();
    }

    /**
     * Create an instance of {@link PathsType.Voltdbroot }
     * 
     */
    public PathsType.Voltdbroot createPathsTypeVoltdbroot() {
        return new PathsType.Voltdbroot();
    }

    /**
     * Create an instance of {@link AdminModeType }
     * 
     */
    public AdminModeType createAdminModeType() {
        return new AdminModeType();
    }

    /**
     * Create an instance of {@link UsersType.User }
     * 
     */
    public UsersType.User createUsersTypeUser() {
        return new UsersType.User();
    }

    /**
     * Create an instance of {@link SystemSettingsType.Temptables }
     * 
     */
    public SystemSettingsType.Temptables createSystemSettingsTypeTemptables() {
        return new SystemSettingsType.Temptables();
    }

    /**
     * Create an instance of {@link ClusterType }
     * 
     */
    public ClusterType createClusterType() {
        return new ClusterType();
    }

    /**
     * Create an instance of {@link SystemSettingsType.Snapshot }
     * 
     */
    public SystemSettingsType.Snapshot createSystemSettingsTypeSnapshot() {
        return new SystemSettingsType.Snapshot();
    }

    /**
     * Create an instance of {@link HttpdType }
     * 
     */
    public HttpdType createHttpdType() {
        return new HttpdType();
    }

    /**
     * Create an instance of {@link HttpdType.Jsonapi }
     * 
     */
    public HttpdType.Jsonapi createHttpdTypeJsonapi() {
        return new HttpdType.Jsonapi();
    }

    /**
     * Create an instance of {@link PathsType }
     * 
     */
    public PathsType createPathsType() {
        return new PathsType();
    }

    /**
     * Create an instance of {@link CommandLogType }
     * 
     */
    public CommandLogType createCommandLogType() {
        return new CommandLogType();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link DeploymentType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "", name = "deployment")
    public JAXBElement<DeploymentType> createDeployment(DeploymentType value) {
        return new JAXBElement<DeploymentType>(_Deployment_QNAME, DeploymentType.class, null, value);
    }

}