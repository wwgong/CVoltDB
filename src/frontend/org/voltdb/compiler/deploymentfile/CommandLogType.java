//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vJAXB 2.1.10 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2011.10.17 at 06:12:41 PM EDT 
//


package org.voltdb.compiler.deploymentfile;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for commandLogType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="commandLogType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;all>
 *         &lt;element name="frequency" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;attribute name="time" type="{}logFrequencyTimeType" default="200" />
 *                 &lt;attribute name="transactions" type="{}logFrequencyTxnsType" default="2147483647" />
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *       &lt;/all>
 *       &lt;attribute name="synchronous" type="{http://www.w3.org/2001/XMLSchema}boolean" default="false" />
 *       &lt;attribute name="enabled" type="{http://www.w3.org/2001/XMLSchema}boolean" default="true" />
 *       &lt;attribute name="logsize" type="{}logSizeType" default="1024" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "commandLogType", propOrder = {

})
public class CommandLogType {

    protected CommandLogType.Frequency frequency;
    @XmlAttribute
    protected Boolean synchronous;
    @XmlAttribute
    protected Boolean enabled;
    @XmlAttribute
    protected Integer logsize;

    /**
     * Gets the value of the frequency property.
     * 
     * @return
     *     possible object is
     *     {@link CommandLogType.Frequency }
     *     
     */
    public CommandLogType.Frequency getFrequency() {
        return frequency;
    }

    /**
     * Sets the value of the frequency property.
     * 
     * @param value
     *     allowed object is
     *     {@link CommandLogType.Frequency }
     *     
     */
    public void setFrequency(CommandLogType.Frequency value) {
        this.frequency = value;
    }

    /**
     * Gets the value of the synchronous property.
     * 
     * @return
     *     possible object is
     *     {@link Boolean }
     *     
     */
    public boolean isSynchronous() {
        if (synchronous == null) {
            return false;
        } else {
            return synchronous;
        }
    }

    /**
     * Sets the value of the synchronous property.
     * 
     * @param value
     *     allowed object is
     *     {@link Boolean }
     *     
     */
    public void setSynchronous(Boolean value) {
        this.synchronous = value;
    }

    /**
     * Gets the value of the enabled property.
     * 
     * @return
     *     possible object is
     *     {@link Boolean }
     *     
     */
    public boolean isEnabled() {
        if (enabled == null) {
            return true;
        } else {
            return enabled;
        }
    }

    /**
     * Sets the value of the enabled property.
     * 
     * @param value
     *     allowed object is
     *     {@link Boolean }
     *     
     */
    public void setEnabled(Boolean value) {
        this.enabled = value;
    }

    /**
     * Gets the value of the logsize property.
     * 
     * @return
     *     possible object is
     *     {@link Integer }
     *     
     */
    public int getLogsize() {
        if (logsize == null) {
            return  1024;
        } else {
            return logsize;
        }
    }

    /**
     * Sets the value of the logsize property.
     * 
     * @param value
     *     allowed object is
     *     {@link Integer }
     *     
     */
    public void setLogsize(Integer value) {
        this.logsize = value;
    }


    /**
     * <p>Java class for anonymous complex type.
     * 
     * <p>The following schema fragment specifies the expected content contained within this class.
     * 
     * <pre>
     * &lt;complexType>
     *   &lt;complexContent>
     *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
     *       &lt;attribute name="time" type="{}logFrequencyTimeType" default="200" />
     *       &lt;attribute name="transactions" type="{}logFrequencyTxnsType" default="2147483647" />
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     * 
     * 
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "")
    public static class Frequency {

        @XmlAttribute
        protected Integer time;
        @XmlAttribute
        protected Integer transactions;

        /**
         * Gets the value of the time property.
         * 
         * @return
         *     possible object is
         *     {@link Integer }
         *     
         */
        public int getTime() {
            if (time == null) {
                return  200;
            } else {
                return time;
            }
        }

        /**
         * Sets the value of the time property.
         * 
         * @param value
         *     allowed object is
         *     {@link Integer }
         *     
         */
        public void setTime(Integer value) {
            this.time = value;
        }

        /**
         * Gets the value of the transactions property.
         * 
         * @return
         *     possible object is
         *     {@link Integer }
         *     
         */
        public int getTransactions() {
            if (transactions == null) {
                return  2147483647;
            } else {
                return transactions;
            }
        }

        /**
         * Sets the value of the transactions property.
         * 
         * @param value
         *     allowed object is
         *     {@link Integer }
         *     
         */
        public void setTransactions(Integer value) {
            this.transactions = value;
        }

    }

}
