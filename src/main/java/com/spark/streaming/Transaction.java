package com.spark.streaming;

import java.io.Serializable;

/**
 * Created by cloudera on 10/31/17.
 */
public class Transaction implements Serializable {

    private String id;
    private String time_stamp;
    private String sp_id;
    private String service_provider;
    private String app_id;
    private String app_name;
    private String state_app;
    private String source_entity_address;
    private String source_entity_masked;
    private String channel_type;

    private String source_protocol;
    private String dest_address;
    private String dest_masked;
    private String dest_channel_type;
    private String dest_protocol;
    private String travel_direction;
    private String ncs;
    private String billing;
    private String part_entity_type;
    private String charge_amount;

    private String currency;
    private String exchange_rates;
    private String charging_service_code;
    private String msisdn;
    private String masked_msisdn;
    private String billing_event;
    private String response_code;
    private String response_desc;
    private String transaction_state;
    private String transaction_keyword;

    private String col_31;
    private String col_32;
    private String col_33;
    private String col_34;
    private String col_35;
    private String col_36;
    private String col_37;
    private String col_38;
    private String col_39;
    private String col_40;

    private String col_41;
    private String col_42;
    private String col_43;
    private String col_44;
    private String col_45;
    private String col_46;
    private String col_47;
    private String col_48;
    private String col_49;
    private String col_50;

    private String col_51;
    private String col_52;
    private String col_53;
    private String col_54;
    private String col_55;
    private String col_56;
    private String col_57;
    private String col_58;
    private String col_59;
    private String col_60;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTime_stamp() {

        return this.time_stamp;
    }

    public void setTime_stamp(String time_stamp) {
        this.time_stamp = time_stamp;
    }

    public String getSp_id() {
        return sp_id;
    }

    public void setSp_id(String sp_id) {
        this.sp_id = sp_id;
    }

    public String getService_provider() {
        return service_provider;
    }

    public void setService_provider(String service_provider) {
        this.service_provider = service_provider;
    }

    public String getApp_id() {
        return app_id;
    }

    public void setApp_id(String app_id) {
        this.app_id = app_id;
    }

    public String getApp_name() {
        return app_name;
    }

    public void setApp_name(String app_name) {
        this.app_name = app_name;
    }

    public String getState_app() {
        return state_app;
    }

    public void setState_app(String state_app) {
        this.state_app = state_app;
    }

    public String getSource_entity_address() {
        return source_entity_address;
    }

    public void setSource_entity_address(String source_entity_address) {
        this.source_entity_address = source_entity_address;
    }

    public String getSource_entity_masked() {
        return source_entity_masked;
    }

    public void setSource_entity_masked(String source_entity_masked) {
        this.source_entity_masked = source_entity_masked;
    }

    public String getChannel_type() {
        return channel_type;
    }

    public void setChannel_type(String channel_type) {
        this.channel_type = channel_type;
    }

    public String getSource_protocol() {
        return source_protocol;
    }

    public void setSource_protocol(String source_protocol) {
        this.source_protocol = source_protocol;
    }

    public String getDest_address() {
        return dest_address;
    }

    public void setDest_address(String dest_address) {
        this.dest_address = dest_address;
    }

    public String getDest_masked() {
        return dest_masked;
    }

    public void setDest_masked(String dest_masked) {
        this.dest_masked = dest_masked;
    }

    public String getDest_channel_type() {
        return dest_channel_type;
    }

    public void setDest_channel_type(String dest_channel_type) {
        this.dest_channel_type = dest_channel_type;
    }

    public String getDest_protocol() {
        return dest_protocol;
    }

    public void setDest_protocol(String dest_protocol) {
        this.dest_protocol = dest_protocol;
    }

    public String getTravel_direction() {
        return travel_direction;
    }

    public void setTravel_direction(String travel_direction) {
        this.travel_direction = travel_direction;
    }

    public String getNcs() {
        return ncs;
    }

    public void setNcs(String ncs) {
        this.ncs = ncs;
    }

    public String getBilling() {
        return billing;
    }

    public void setBilling(String billing) {
        this.billing = billing;
    }

    public String getPart_entity_type() {
        return part_entity_type;
    }

    public void setPart_entity_type(String part_entity_type) {
        this.part_entity_type = part_entity_type;
    }

    public String getCharge_amount() {
        return charge_amount;
    }

    public void setCharge_amount(String charge_amount) {
        this.charge_amount = charge_amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getExchange_rates() {
        return exchange_rates;
    }

    public void setExchange_rates(String exchange_rates) {
        this.exchange_rates = exchange_rates;
    }

    public String getCharging_service_code() {
        return charging_service_code;
    }

    public void setCharging_service_code(String charging_service_code) {
        this.charging_service_code = charging_service_code;
    }

    public String getMsisdn() {
        return msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    public String getMasked_msisdn() {
        return masked_msisdn;
    }

    public void setMasked_msisdn(String masked_msisdn) {
        this.masked_msisdn = masked_msisdn;
    }

    public String getBilling_event() {
        return billing_event;
    }

    public void setBilling_event(String billing_event) {
        this.billing_event = billing_event;
    }

    public String getResponse_code() {
        return response_code;
    }

    public void setResponse_code(String response_code) {
        this.response_code = response_code;
    }

    public String getResponse_desc() {
        return response_desc;
    }

    public void setResponse_desc(String response_desc) {
        this.response_desc = response_desc;
    }

    public String getTransaction_state() {
        return transaction_state;
    }

    public void setTransaction_state(String transaction_state) {
        this.transaction_state = transaction_state;
    }

    public String getTransaction_keyword() {
        return transaction_keyword;
    }

    public void setTransaction_keyword(String transaction_keyword) {
        this.transaction_keyword = transaction_keyword;
    }

    public String getCol_31() {
        return col_31;
    }

    public void setCol_31(String col_31) {
        this.col_31 = col_31;
    }

    public String getCol_32() {
        return col_32;
    }

    public void setCol_32(String col_32) {
        this.col_32 = col_32;
    }

    public String getCol_33() {
        return col_33;
    }

    public void setCol_33(String col_33) {
        this.col_33 = col_33;
    }

    public String getCol_34() {
        return col_34;
    }

    public void setCol_34(String col_34) {
        this.col_34 = col_34;
    }

    public String getCol_35() {
        return col_35;
    }

    public void setCol_35(String col_35) {
        this.col_35 = col_35;
    }

    public String getCol_36() {
        return col_36;
    }

    public void setCol_36(String col_36) {
        this.col_36 = col_36;
    }

    public String getCol_37() {
        return col_37;
    }

    public void setCol_37(String col_37) {
        this.col_37 = col_37;
    }

    public String getCol_38() {
        return col_38;
    }

    public void setCol_38(String col_38) {
        this.col_38 = col_38;
    }

    public String getCol_39() {
        return col_39;
    }

    public void setCol_39(String col_39) {
        this.col_39 = col_39;
    }

    public String getCol_40() {
        return col_40;
    }

    public void setCol_40(String col_40) {
        this.col_40 = col_40;
    }

    public String getCol_41() {
        return col_41;
    }

    public void setCol_41(String col_41) {
        this.col_41 = col_41;
    }

    public String getCol_42() {
        return col_42;
    }

    public void setCol_42(String col_42) {
        this.col_42 = col_42;
    }

    public String getCol_43() {
        return col_43;
    }

    public void setCol_43(String col_43) {
        this.col_43 = col_43;
    }

    public String getCol_44() {
        return col_44;
    }

    public void setCol_44(String col_44) {
        this.col_44 = col_44;
    }

    public String getCol_45() {
        return col_45;
    }

    public void setCol_45(String col_45) {
        this.col_45 = col_45;
    }

    public String getCol_46() {
        return col_46;
    }

    public void setCol_46(String col_46) {
        this.col_46 = col_46;
    }

    public String getCol_47() {
        return col_47;
    }

    public void setCol_47(String col_47) {
        this.col_47 = col_47;
    }

    public String getCol_48() {
        return col_48;
    }

    public void setCol_48(String col_48) {
        this.col_48 = col_48;
    }

    public String getCol_49() {
        return col_49;
    }

    public void setCol_49(String col_49) {
        this.col_49 = col_49;
    }

    public String getCol_50() {
        return col_50;
    }

    public void setCol_50(String col_50) {
        this.col_50 = col_50;
    }

    public String getCol_51() {
        return col_51;
    }

    public void setCol_51(String col_51) {
        this.col_51 = col_51;
    }

    public String getCol_52() {
        return col_52;
    }

    public void setCol_52(String col_52) {
        this.col_52 = col_52;
    }

    public String getCol_53() {
        return col_53;
    }

    public void setCol_53(String col_53) {
        this.col_53 = col_53;
    }

    public String getCol_54() {
        return col_54;
    }

    public void setCol_54(String col_54) {
        this.col_54 = col_54;
    }

    public String getCol_55() {
        return col_55;
    }

    public void setCol_55(String col_55) {
        this.col_55 = col_55;
    }

    public String getCol_56() {
        return col_56;
    }

    public void setCol_56(String col_56) {
        this.col_56 = col_56;
    }

    public String getCol_57() {
        return col_57;
    }

    public void setCol_57(String col_57) {
        this.col_57 = col_57;
    }

    public String getCol_58() {
        return col_58;
    }

    public void setCol_58(String col_58) {
        this.col_58 = col_58;
    }

    public String getCol_59() {
        return col_59;
    }

    public void setCol_59(String col_59) {
        this.col_59 = col_59;
    }

    public String getCol_60() {
        return col_60;
    }

    public void setCol_60(String col_60) {
        this.col_60 = col_60;
    }


}
