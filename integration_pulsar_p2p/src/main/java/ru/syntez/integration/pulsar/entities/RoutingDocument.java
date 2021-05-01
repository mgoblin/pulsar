package ru.syntez.integration.pulsar.entities;

import lombok.Data;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

/**
 * RoutingDocument model
 *
 * @author Skyhunter
 */
@XmlRootElement(name = "routingDocument")
@XmlAccessorType(XmlAccessType.FIELD)
@Data
public class RoutingDocument implements Serializable {

    private DocumentTypeEnum docType;
    private int docId;

    public static RoutingDocument createUnknown(int id) {
        final RoutingDocument document = new RoutingDocument();
        document.setDocId(id);
        document.setDocType(DocumentTypeEnum.unknown);
        return document;
    }

    public static RoutingDocument createOrder(int id) {
        final RoutingDocument document = new RoutingDocument();
        document.setDocId(id);
        document.setDocType(DocumentTypeEnum.order);
        return document;
    }

    public static RoutingDocument createInvoice(int id) {
        final RoutingDocument document = new RoutingDocument();
        document.setDocId(id);
        document.setDocType(DocumentTypeEnum.invoice);
        return document;
    }
}
