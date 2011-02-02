package org.commoncrawl.rpc.compiler;

import java.util.ArrayList;

public class JModule {

  public String              _moduleName;
  public ArrayList<JRecord>  _records  = new ArrayList<JRecord>();
  public ArrayList<JService> _services = new ArrayList<JService>();

  public JModule(String moduleName) {
    _moduleName = moduleName;
  }

  public void addService(JService service) {
    _services.add(service);
  }

  public void addRecord(JRecord record) {
    _records.add(record);
  }

  public ArrayList<JRecord> getRecords() {
    return _records;
  }

  public ArrayList<JService> getServices() {
    return _services;
  }
}
