/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commoncrawl.rpc.thriftrpc.ant;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.commoncrawl.rpc.compiler.generated.RPCCompiler;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;

/**
 * Hadoop record compiler ant Task
 *<p> This task takes the given record definition files and compiles them into
 * java or c++
 * files. It is then up to the user to compile the generated files.
 *
 * <p> The task requires the <code>file</code> or the nested fileset element to be
 * specified. Optional attributes are <code>language</code> (set the output
 * language, default is "java"),
 * <code>destdir</code> (name of the destination directory for generated java/c++
 * code, default is ".") and <code>failonerror</code> (specifies error handling
 * behavior. default is true).
 * <p><h4>Usage</h4>
 * <pre>
 * &lt;recordcc
 *       destdir="${basedir}/gensrc"
 *       language="java"&gt;
 *   &lt;fileset include="**\/*.jr" /&gt;
 * &lt;/recordcc&gt;
 * </pre>
 */
public class RccTask extends Task {
  
  private String language = "java";
  private File src;
  private File dest = new File(".");
  private final ArrayList<FileSet> filesets = new ArrayList<FileSet>();
  private boolean failOnError = true;
  
  /** Creates a new instance of RccTask */
  public RccTask() {
  }
  
  /**
   * Sets the output language option
   * @param language "java"/"c++"
   */
  public void setLanguage(String language) {
    this.language = language;
  }
  
  /**
   * Sets the record definition file attribute
   * @param file record definition file
   */
  public void setFile(File file) {
    this.src = file;
  }
  
  /**
   * Given multiple files (via fileset), set the error handling behavior
   * @param flag true will throw build exception in case of failure (default)
   */
  public void setFailonerror(boolean flag) {
    this.failOnError = flag;
  }
  
  /**
   * Sets directory where output files will be generated
   * @param dir output directory
   */
  public void setDestdir(File dir) {
    this.dest = dir;
  }
  
  /**
   * Adds a fileset that can consist of one or more files
   * @param set Set of record definition files
   */
  public void addFileset(FileSet set) {
    filesets.add(set);
  }

  /** gen stamp **/
  File getGenStampFile(File baseDir) { 
    return new File(baseDir,"thrift-gen.stamp");
  }
  
  /**
   * Invoke the Hadoop record compiler on each record definition file
   */
  public void execute() throws BuildException {
    
    System.out.println("Thrif RCC Execute");
    
    if (src == null && filesets.size()==0) {
      throw new BuildException("There must be a file attribute or a fileset child element");
    }
    
    ArrayList<File> srcList = new ArrayList<File>();
    if (src != null) {
      System.out.println("Src is present:" + src.getAbsolutePath());
      srcList.add(src);
    }
    
    Project myProject = getProject();
    
    for (int i = 0; i < filesets.size(); i++) {
      FileSet fs = filesets.get(i);
      DirectoryScanner ds = fs.getDirectoryScanner(myProject);
      File dir = fs.getDir(myProject);
      String[] srcs = ds.getIncludedFiles();
      for (int j = 0; j < srcs.length; j++) {
        File srcLocation = new File(dir,srcs[j]);
        srcList.add(srcLocation);
      }
    }
    for (File srcLocation : srcList) {
        File genStampFile = getGenStampFile(srcLocation.getParentFile());
      if (!genStampFile.exists() || srcLocation.lastModified() > genStampFile.lastModified()) {
          
          System.out.println("src:" + srcLocation.getAbsolutePath());
          
          doCompile(srcLocation);
          
          genStampFile.delete();
          
          try { 
            genStampFile.createNewFile();
          }
          catch (IOException e) { 
            throw new BuildException("Failed to create GenStamp File for src:" + srcLocation);
          }
      }
    }
  }
  
  
  private File getDestFileGivenFQN(String fullyQualifiedName) { 
    String pathDelimited = fullyQualifiedName.replace(".", "/");
    return new File(dest,pathDelimited + ".java");
  }
  
  private void doCompile(File file) throws BuildException {
    
    Pattern namespacePattern = Pattern.compile("namespace\\s+java\\s+(.+)$");
    Pattern servicePattern = Pattern.compile("service\\s+([^\\s{]+).*$");
    try {
      BufferedReader reader = new BufferedReader(new FileReader(file));
      try { 
        String line = null;
        String namespace = null;
        while ((line = reader.readLine()) != null) { 
          Matcher namespaceMatcher = namespacePattern.matcher(line);
          Matcher servicePatternMatcher = servicePattern.matcher(line);
          
          if (namespaceMatcher.matches()) { 
            namespace = namespaceMatcher.group(1);
            System.out.println("Namespace is:" + namespace);
          }
          else if (servicePatternMatcher.matches()) { 
            String service = servicePatternMatcher.group(1);
            System.out.println("Processing Service :" + service);
            
            String fullyQualifiedName = namespace + "." + service;
            File destFile = getDestFileGivenFQN(fullyQualifiedName + "_CCAsyncSupport");
            destFile.getParentFile().mkdirs();
            System.out.println("Dest File:" + destFile.getAbsolutePath());
            PrintWriter writer = new PrintWriter(new FileWriter(destFile));
            
            try {
              generateAsyncStub(fullyQualifiedName,writer);
            } catch (ClassNotFoundException e) {
              e.printStackTrace();
            }
            finally { 
              writer.flush();
              writer.close();
            }
          }
        }
      }
      finally { 
        reader.close();
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      throw new BuildException(e);
    } catch (IOException e) {
      e.printStackTrace();
      throw new BuildException(e);
    }
    
  }
  
  public static void generateAsyncStub(String fullyQualifiedServiceName,PrintWriter writer) throws ClassNotFoundException {
    String packageName = fullyQualifiedServiceName.substring(0,fullyQualifiedServiceName.lastIndexOf("."));
    String className = fullyQualifiedServiceName.substring(fullyQualifiedServiceName.lastIndexOf(".") + 1);
    Class zclass = Class.forName(fullyQualifiedServiceName+"$Iface");

    writer.println("package " + packageName +";");
    writer.println("");
    writer.println("import java.util.List;");
    writer.println("import java.util.ArrayList;");
    writer.println("import java.util.Map;");
    writer.println("import java.util.HashMap;");
    writer.println("import java.util.EnumMap;");
    writer.println("import java.util.Set;");
    writer.println("import java.util.HashSet;");
    writer.println("import java.util.EnumSet;");
    writer.println("import java.util.Collections;");
    writer.println("import java.util.BitSet;");
    writer.println("import java.nio.ByteBuffer;");
    writer.println("import java.util.Arrays;");
    writer.println("import org.slf4j.Logger;");
    writer.println("import org.slf4j.LoggerFactory;");
    writer.println("import java.io.IOException;");

    writer.println("");
    writer.println("import org.commoncrawl.rpc.thriftrpc.*;");
    writer.println("import org.apache.thrift.TException;");
    writer.println("import org.apache.thrift.protocol.TMessage;");
    writer.println("import org.apache.thrift.protocol.TProtocol;");
    writer.println("import org.apache.thrift.transport.TFramedTransport;");
    writer.println("import org.apache.thrift.transport.TIOStreamTransport;");
    writer.println("import org.commoncrawl.io.internal.NIOBufferList;");
    writer.println("import org.commoncrawl.io.internal.NIOBufferListOutputStream;");
    writer.println("import org.commoncrawl.rpc.thriftrpc.*;");
    writer.println("import org.commoncrawl.rpc.thriftrpc.ThriftAsyncRequestProcessor;");
    writer.println("import org.commoncrawl.rpc.thriftrpc.ThriftAsyncRemoteCallContext;");
    writer.println("import org.commoncrawl.rpc.thriftrpc.ThriftAsyncRequest;");
    writer.println("import org.commoncrawl.rpc.thriftrpc.ThriftAsyncRemoteClientChannel;");
    writer.println("import org.commoncrawl.rpc.thriftrpc.ThriftAsyncServerChannel;");
    writer.println("import " + packageName + "." + className +".*;");

    writer.println("class " + className +"_CCAsyncSupport {");
    writer.println("");
    writer.println("  public static class " + className + "_AsyncCallStub extends ThriftAsyncClientChannel.AsyncStub {");
    writer.println("    public " + className+ "_AsyncCallStub(ThriftAsyncClientChannel channel) {");
    writer.println("      super(channel);");
    writer.println("    }");
    writer.println("");
    // get Blocking IFace 
    Class blockingIFaceClass = Class.forName(fullyQualifiedServiceName + "$Iface");

    for (Method m : blockingIFaceClass.getMethods()) {
      
      String argsName = m.getName() + "_args";
      String resultName = m.getName() + "_result";
      
      Class argsClass = Class.forName(fullyQualifiedServiceName + "$" + argsName);
      Map<? extends TFieldIdEnum, FieldMetaData> metadataMap = FieldMetaData.getStructMetaDataMap(argsClass);
      
      writer.print("    public void " + m.getName() +"(");
      int fieldIndex = 0;
      for (TFieldIdEnum field : metadataMap.keySet()) { 
        if (fieldIndex++ != 0) 
          writer.print(",");
        try {
          writer.print(argsClass.getField(field.getFieldName()).getType().getName() + " " + field.getFieldName());
        } catch (SecurityException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (NoSuchFieldException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      writer.println(",final ThriftAsyncRequest.ThriftAsyncRequestCallback<" + argsName + "," + resultName +"> resultHandler) throws org.apache.thrift.TException {");
      writer.println("      " + argsName + " args = new " + argsName +"();");
      for (TFieldIdEnum field : metadataMap.keySet()) { 
        try {
          writer.println("      args." + field.getFieldName() + " = " + field.getFieldName() +";");
        } catch (SecurityException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      writer.println("      " + resultName + " result = new " + resultName + "();");
      writer.println("      ThriftAsyncRequest<" + argsName + "," + resultName + "> asyncCallContext = " +
          "new ThriftAsyncRequest<"+ argsName + "," + resultName + ">(super.getChannel(),resultHandler,\""+ m.getName() + "\",args,result);");
      writer.println("      super.getChannel().sendRequest(asyncCallContext);");
      writer.println("    }");
    }
    writer.println("  }");
              
    String baseInterfaceName = className +"_AsyncIFace";
    writer.println("  public static interface " + baseInterfaceName +"{ ");
    for (Method method : zclass.getMethods()) {
      String inputType = method.getName()+"_args";
      String outputType = method.getName()+"_result";

      writer.println("    public void " + className +"_" + method.getName() +" (final ThriftAsyncRemoteCallContext<" + inputType + "," + outputType + "> context) throws TException;");
    }
    writer.println("  }");
    String processClassName = className +"_RequestProcessor";

    writer.println("  public static class " + processClassName +" implements ThriftAsyncRequestProcessor { ");
    writer.println("    " + baseInterfaceName + " _iface;");
    writer.println("    " + processClassName+"(" + baseInterfaceName +" iface){");
    writer.println("      _iface = iface;");
    writer.println("    }");
    writer.println("");
    writer.println("   public void process(ThriftAsyncServerChannel serverChannel,ThriftAsyncRemoteClientChannel clientChannel,TMessage message,TProtocol iprot)throws TException {");

    for (Method method : zclass.getMethods()) {
      String inputType = method.getName()+"_args";
      String outputType = method.getName()+"_result";
      
      writer.println("    if (message.name.equals(\"" + method.getName() +"\")){");
      writer.println("      " + inputType +" input = new "+ inputType +"();");
      writer.println("      " + outputType +" output = new "+ outputType +"();");
      writer.println("      input.read(iprot);");
      writer.println("      iprot.readMessageEnd();");
      
      writer.println("      ThriftAsyncRemoteCallContext<" + inputType + "," + outputType +"> context = new ThriftAsyncRemoteCallContext<" + inputType + "," + outputType +">" +
            "(serverChannel,clientChannel,message,iprot,input,output);");
      writer.println("      _iface." + className +"_" + method.getName() +"(context);");
      writer.println("      return;");
      writer.println("    }");
    }
    writer.println("    org.apache.thrift.protocol.TProtocolUtil.skip(iprot, org.apache.thrift.protocol.TType.STRUCT);");
    writer.println("    iprot.readMessageEnd();");
    writer.println("    NIOBufferList bufferListTemp = new NIOBufferList();");
    writer.println("    TProtocol oprot = clientChannel.getOutputProtocolFactory().getProtocol(new TFramedTransport(new TIOStreamTransport(new NIOBufferListOutputStream(bufferListTemp))));");

    writer.println("    org.apache.thrift.TApplicationException x = new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.UNKNOWN_METHOD, \"Invalid method name: '+message.name+'\");");
    writer.println("    oprot.writeMessageBegin(new org.apache.thrift.protocol.TMessage(message.name, org.apache.thrift.protocol.TMessageType.EXCEPTION, message.seqid));");
    writer.println("    x.write(oprot);");
    writer.println("    oprot.writeMessageEnd();");
    writer.println("    oprot.getTransport().flush();");
    writer.println("    try {");
    writer.println("      clientChannel.responseReady(bufferListTemp);");
    writer.println("    } catch (IOException e) {");
    writer.println("      throw new TException(e);");
    writer.println("    }");

    writer.println("    }");
    writer.println("");
    writer.println("  }");
    writer.println("");
    writer.println("}");

  }  
}
