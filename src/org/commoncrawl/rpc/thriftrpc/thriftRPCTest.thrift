namespace java org.commoncrawl.rpc.thriftrpc

#
# data structures / enumerations 
#

enum EnumeratedValue { 
  ONE,
  TWO
}

struct ThriftUnitTestStruct1 {
 1: required i32     intType; 
 2: required i64    longType;
 3: required string  stringType;
 4: list<string>     listOfStrings;
}

#
# service definitions  
# 

service ThriftUnitTest {
  ThriftUnitTestStruct1 hello(1:ThriftUnitTestStruct1 input 2:string paramTwo);
  void hello2(1:ThriftUnitTestStruct1 input);
}
