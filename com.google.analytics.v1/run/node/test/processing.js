const processor = require('../index.js');
const expect = require('chai').expect;
const _ = require('lodash')
const fs = require('fs');
const avro = require('avsc');

var printError = function(error, explicit) {
   console.log(`[${explicit ? 'EXPLICIT' : 'INEXPLICIT'}] ${error.name}: ${error.message}`);
}

const avroString = fs.readFileSync('./com.google.analytics.v1.Entity.avsc', "utf8");

try {
   JSON.parse(avroString);
} catch (e) {
   if (e instanceof SyntaxError) {
      printError(e, true);
      var index = parseInt(e.message.replace(/\n/, '').match(/.*position.([0-9]{1,5})$/)[1]);
      // print where in the avro schema the error is comin from
      console.log(avroString.substring(index-50, index) + '^^^' + avroString.substring(index, index+50));
  } else {
      printError(e, false);
  }
}

//console.log(avroString);
const registry = {}; // Registry where new types get added.
const unionType = avro.Type.forSchema(JSON.parse(avroString), {registry});
inputType = registry['com.google.analytics.v1.transformed.Entity'];
outputType = registry['com.google.analytics.v1.Entity'];
console.log(registry);

// tests based on common hits in https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#commonhits
describe('#processing()', function() {
    var request = {
        params: {
            topic: 'dummy'
        },
        body: {
            message: {
                attributes : { 
                    name: 'Entity', 
                    namespace: 'com.google.analytics.v1.transformed', 
                    timestamp: '2020-07-16T06:08:07.077Z', 
                    topic: 'serialized', 
                    uuid: '63958418-b0f4-4007-b639-fe4698ffe220'
                }
            }
        }
    };

    var payload = 'MDIwMjAtMDgtMDdUMDY6NDA6NDkuODkyWgIxAigyNTU0MjQzNzUuMTU5NjYwNTEzNAACEHBhZ2V2aWV3AgAAAhQyd2c3djE5UThCGlVBLTIzMzQwNTY2LTEAAAIChgFVbmxpbWl0ZWQgcGVyc2lzdGVudCBkaXNrIGluIGdvb2dsZSBjbG91ZCBzaGVsbCDCtyByb2JlcnRzYWhsaW4uY29tApIBaHR0cHM6Ly9yb2JlcnRzYWhsaW4uY29tL3VubGltaXRlZC1wZXJzaXN0ZW50LWRpc2staW4tZ29vZ2xlLWNsb3VkLXNoZWxsLwIgcm9iZXJ0c2FobGluLmNvbQJiL3VubGltaXRlZC1wZXJzaXN0ZW50LWRpc2staW4tZ29vZ2xlLWNsb3VkLXNoZWxsLwKSAWh0dHBzOi8vcm9iZXJ0c2FobGluLmNvbS91bmxpbWl0ZWQtcGVyc2lzdGVudC1kaXNrLWluLWdvb2dsZS1jbG91ZC1zaGVsbC8AAAAAAgLoAU1vemlsbGEvNS4wIChYMTE7IENyT1MgYWFyY2g2NCAxMzA5OS44NS4wKSBBcHBsZVdlYktpdC81MzcuMzYgKEtIVE1MLCBsaWtlIEdlY2tvKSBDaHJvbWUvODQuMC40MTQ3LjExMCBTYWZhcmkvNTM3LjM2AgACAAICDDI0LWJpdAIQMTUzNng4NjQCAgRTRQIEYWICEnN0b2NraG9sbQAAAAAAAgAAAAAAAAAAAAAAAAACAAAAAA=='

    var expectedTemplate = 
      {
         attributes:{ 
            name: 'Hit',
            namespace: 'com.google.analytics.v1',
            timestamp: '2020-07-16T06:08:07.077Z',
            uuid: '63958418-b0f4-4007-b639-fe4698ffe220',
            topic: 'dummy'
         },
         data:{
            timestamp: '2020-07-16T06:08:07.077Z',
            version: '1',
            clientId: '555',
            hit: {
               type: 'pageview', 
               gtmContainerId: '3uf7839Q8B',
               propertyId: 'UA-12345-6',
               nonInteraction: false
            },
            content:{ 
               referer: 'https://mydemo.com/',
               contentGroups: null
            },
            event: null,
            device:{ 
               browser:{ 
                  viewportSize: '728x714',
                  name: 'Chrome',
                  version: '83.0.4103.119',
                  versionMajor: '83',
                  engineName: 'Blink',
                  engineVersion: '83.0.4103.119',
                  javaEnabled: false
               },
               operatingsystem:{
                  encoding: 'UTF-8',
                  language: 'en-us',
                  name: 'Chromium OS',
                  version: '13020.87.0'
               },
               screen: {
                  colors: '24-bit', 
                  resolution: '1536x864'
               },
               userAgent: 'Mozilla/5.0 (X11; CrOS aarch64 13020.87.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.119 Safari/537.36',
            },
            trafficSource:{ 
               utm: null,
               campaign: null,
            },
            geo:{ 
               country: 'SE',
               region: 'ab',
               city: 'ronninge',
               latitude: 59.194337,
               longitude: 17.748790
            },
            experiments:[],
            exception: null,
            latency:{
               timing: null,
            },
            social: null,
            app: null,
            customDimensions: null,
            customMetrics: null,
            ecommerce:{ 
               products: [],
               promotions: [],
               transaction: null,
               checkout: null
            }
         }
      };

   it('traffic source tracking with document host (dh) and document page (dp)', function() {
      request.body.message.data = payload;
      var processed = processor.processing(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult.attributes.uuid = processed.attributes.uuid;
      expectedResult.data.content.url = 'mydemo.com/home?utm_source=google&utm_medium=cpc&utm_campaign=spring_sale&utm_term=spring%2Csale%2Crunning&utm_content=running_shoe&utm_id=123';
      expectedResult.data.content.hostname = 'mydemo.com';
      expectedResult.data.content.path = '/home?utm_source=google&utm_medium=cpc&utm_campaign=spring_sale&utm_term=spring%2Csale%2Crunning&utm_content=running_shoe&utm_id=123';
      expectedResult.data.content.title = 'homepage';
      expectedResult.data.trafficSource = {
         campaign: {
            id : '123',
            name : 'spring_sale',
            medium : 'cpc',
            content : 'running_shoe',
            source : 'google',
            keyword : 'spring,sale,running'
         },
         utm:{
            id : '123',
            name : 'spring_sale',
            medium : 'cpc',
            content : 'running_shoe',
            source : 'google',
            keyword : 'spring,sale,running'
         },
         gclId : '123456',
         dclId : '789',
         referer : 'https://google.com'
      };
      expect(processed).to.deep.equal(expectedResult); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });
   
});