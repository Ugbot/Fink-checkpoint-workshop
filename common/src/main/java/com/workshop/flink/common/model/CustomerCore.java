package com.workshop.flink.common.model;

import java.io.Serializable;

/**
 * Partial-update source: the KYC subsystem only knows about identity + residency
 * columns. Written into the wide {@code customer_360} Fluss PK table where it
 * fills in only the {@code kyc_*} subset of the wider schema; all other
 * columns are left untouched.
 */
public class CustomerCore implements Serializable {

    private static final long serialVersionUID = 1L;

    private String customerId;       // primary key (matches customer_360 PK)
    private String legalName;
    private String dateOfBirth;
    private String nationality;
    private String taxResidency;
    private String kycStatus;        // PENDING / VERIFIED / EXPIRED / REJECTED
    private long   kycVerifiedAt;
    private String idDocumentType;   // PASSPORT / NATIONAL_ID / DRIVERS_LICENCE
    private String idDocumentNumber;
    private String addressLine;
    private String addressCountry;
    private long   updatedAt;

    public CustomerCore() {}

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String v) { this.customerId = v; }
    public String getLegalName() { return legalName; }
    public void setLegalName(String v) { this.legalName = v; }
    public String getDateOfBirth() { return dateOfBirth; }
    public void setDateOfBirth(String v) { this.dateOfBirth = v; }
    public String getNationality() { return nationality; }
    public void setNationality(String v) { this.nationality = v; }
    public String getTaxResidency() { return taxResidency; }
    public void setTaxResidency(String v) { this.taxResidency = v; }
    public String getKycStatus() { return kycStatus; }
    public void setKycStatus(String v) { this.kycStatus = v; }
    public long getKycVerifiedAt() { return kycVerifiedAt; }
    public void setKycVerifiedAt(long v) { this.kycVerifiedAt = v; }
    public String getIdDocumentType() { return idDocumentType; }
    public void setIdDocumentType(String v) { this.idDocumentType = v; }
    public String getIdDocumentNumber() { return idDocumentNumber; }
    public void setIdDocumentNumber(String v) { this.idDocumentNumber = v; }
    public String getAddressLine() { return addressLine; }
    public void setAddressLine(String v) { this.addressLine = v; }
    public String getAddressCountry() { return addressCountry; }
    public void setAddressCountry(String v) { this.addressCountry = v; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long v) { this.updatedAt = v; }
}
