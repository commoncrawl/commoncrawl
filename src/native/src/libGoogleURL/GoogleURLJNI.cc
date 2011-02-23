/*
 *  GoogleURLJNI.cc
 *  
 *
 *  Created by Ahad Rana on 9/2/08.
 *  Copyright 2008 CommonCrawl.org. All rights reserved.
 *
 */
#include <jni.h>
#include "org_commoncrawl_util_shared_GoogleURL.h"
#include "googleurl/src/gurl.h"
#include "googleurl/src/url_util.h"
#include <unicode/utf.h>
#include <algorithm>

#pragma GCC visibility push(default)

static jfieldID _isValid = 0;
static jfieldID _scheme = 0;
static jfieldID _userName = 0;
static jfieldID _password = 0;
static jfieldID _host = 0;
static jfieldID _port = 0;
static jfieldID _path = 0;
static jfieldID _query = 0;
static jfieldID _ref = 0;
static jfieldID _canonicalURL = 0;
static jfieldID _componentField_begin=0;
static jfieldID _componentField_len=0;




extern "C"  void JNICALL Java_org_commoncrawl_util_shared_GoogleURL_internal_1init(JNIEnv* env, jclass googleURLClazz,jclass googleURLComponentClazz) { 

  // get the jclass for Class 
  jclass objectClass = env->FindClass("java/lang/Class");
  // get the get cannonical name method id ... 
  jmethodID getNameId = env->GetMethodID(objectClass,"getName","()Ljava/lang/String;");
  
  if (getNameId) { 
  
    // and invoke the method ... 
    jstring theCanoncialName =(jstring) env->CallObjectMethod(googleURLComponentClazz,getNameId);
    
    if (theCanoncialName != 0) { 
      
      // and extract the the utf16 string from the string object ...
      const char* strCanonicalName = env->GetStringUTFChars(theCanoncialName,0);

      std::string name;

      name +="L";
      name += strCanonicalName;
      name += ";";
      
      std::replace_if(name.begin(), name.end(), std::bind2nd(std::equal_to<char>(), '.'), '/');

      
      // printf("cannonical name for class is:%s\n",name.c_str());
      
            
      // initialize the class field ids for the GoogleURL Class
      _isValid  = env->GetFieldID(googleURLClazz,"_isValid","Z");
      _scheme   = env->GetFieldID(googleURLClazz,"_scheme",name.c_str());
      _userName = env->GetFieldID(googleURLClazz,"_userName",name.c_str());
      _password = env->GetFieldID(googleURLClazz,"_password",name.c_str());
      _host     = env->GetFieldID(googleURLClazz,"_host",name.c_str());
      _port     = env->GetFieldID(googleURLClazz,"_port",name.c_str());
      _path     = env->GetFieldID(googleURLClazz,"_path",name.c_str());
      _query    = env->GetFieldID(googleURLClazz,"_query",name.c_str());
      _ref      = env->GetFieldID(googleURLClazz,"_ref",name.c_str());
      _canonicalURL = env->GetFieldID(googleURLClazz,"_canonicalURL","Ljava/lang/String;");
      
      // and next extract the begin and len field ids for the component Class 
      _componentField_begin = env->GetFieldID(googleURLComponentClazz,"begin","I");
      _componentField_len   = env->GetFieldID(googleURLComponentClazz,"len","I");
      
      //printf("got all the fields... freeing canonical name string\n");
      // now free any strings. ... 
      env->ReleaseStringUTFChars(theCanoncialName,strCanonicalName);
      
      //printf("done done dude\n");
      
    }
    else {
      //printf("could not find canonical name method in class object\n");
    }
  }
  else { 
    //printf("did not get getCanonicalName method id\n");
  }
}

int countUCS2CharsInUTF8String(const char* str,int strlen) { 
  int ucs2CharsCount = 0;
  
  int i=0;
  UChar32 wideChar;
  
  while (i < strlen) { 
    U8_NEXT(str,i,strlen,wideChar);
    ++ucs2CharsCount;
  }
  return ucs2CharsCount;
}

void populateJavaComponent(JNIEnv* env,jobject thisObj,jfieldID fieldId,url_parse::Component& component) { 
  
  jobject componentObj = env->GetObjectField(thisObj,fieldId);
  
  env->SetIntField(componentObj,_componentField_begin,component.begin);
  env->SetIntField(componentObj,_componentField_len,component.len);
} 

int populateJavaComponentUTF8Safe(JNIEnv* env,jobject thisObj,jfieldID fieldId,url_parse::Component& component,const std::string& sourceBuffer, int deltaIn) { 
  
  jobject componentObj = env->GetObjectField(thisObj,fieldId);

  // get char* 
  const char* sourceBufferPtr = sourceBuffer.c_str();
  // offset by start position ... 
  sourceBufferPtr += component.begin;
  // calculate wide char len ... 
  int ucs2Len = countUCS2CharsInUTF8String(sourceBufferPtr,component.len);
  // calculate delta 
  int componentDelta = component.len - ucs2Len;
  // adjust component start by passed in delta 
  int adjustedStart = component.begin - deltaIn;
  // set fields ... 
  env->SetIntField(componentObj,_componentField_begin,adjustedStart);
  env->SetIntField(componentObj,_componentField_len,ucs2Len);
  
  // return new delta (if changed)
  
  return deltaIn + componentDelta;
}

extern "C" void JNICALL Java_org_commoncrawl_util_shared_GoogleURL_initializeFromURL(JNIEnv * env, jobject thisObj, jstring urlString) { 

  jboolean iscopy;
  
  int length = env->GetStringLength(urlString);
  //TODO: MUST FREE STRING DUDE !!!
  const url_util::UTF16Char* ptrUrlString = env->GetStringChars(urlString,&iscopy);
  
  std::string cannonical;
  url_parse::Parsed parsed;


  if (length != 0 && ptrUrlString) { 

    cannonical.reserve(length + 32);
    
    url_canon::StdStringCanonOutput output(&cannonical);
    
    bool success = url_util::Canonicalize(ptrUrlString,length,&output, &parsed);
    
    if (success) { 

      output.Complete();  // Must be done before using string.
      
      // printf("successfull canonicalization. output:%s\n",cannonical.c_str());
      
      // allocate a new java String ...
      jstring canonicalStr = env->NewStringUTF(cannonical.c_str());
      // set it in the underlying object ...
      env->SetObjectField(thisObj,_canonicalURL,canonicalStr);

      // track delta values 
      int offsetDelta = 0;
      
      // now populate component members ... 
      if (parsed.scheme.is_valid())    
        populateJavaComponent(env,thisObj,_scheme,parsed.scheme);
      
      if (parsed.username.is_valid())  offsetDelta = populateJavaComponentUTF8Safe(env,thisObj,_userName,parsed.username,cannonical,offsetDelta);
      if (parsed.password.is_valid())  offsetDelta = populateJavaComponentUTF8Safe(env,thisObj,_password,parsed.password,cannonical,offsetDelta);
      if (parsed.host.is_valid())      offsetDelta = populateJavaComponentUTF8Safe(env,thisObj,_host,parsed.host,cannonical,offsetDelta);
      if (parsed.port.is_valid())      offsetDelta = populateJavaComponentUTF8Safe(env,thisObj,_port,parsed.port,cannonical,offsetDelta);
      if (parsed.path.is_valid())      offsetDelta = populateJavaComponentUTF8Safe(env,thisObj,_path,parsed.path,cannonical,offsetDelta);
      if (parsed.query.is_valid())     offsetDelta = populateJavaComponentUTF8Safe(env,thisObj,_query,parsed.query,cannonical,offsetDelta);
      if (parsed.ref.is_valid())       offsetDelta = populateJavaComponentUTF8Safe(env,thisObj,_ref,parsed.ref,cannonical,offsetDelta);
      // and finally set the url valid flag ... 
      env->SetBooleanField(thisObj,_isValid,JNI_TRUE);
    }
    else {
      //printf("url normalization failed!\n");
    }

    // FREE STRING HERE ... 
    env->ReleaseStringChars(urlString,ptrUrlString);
  }    
}

#pragma GCC visibility pop
