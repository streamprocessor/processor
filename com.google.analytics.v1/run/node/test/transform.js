const adapter = require('../index.js');
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
const type = registry['com.google.analytics.v1.raw.Entity'];
console.log(registry);
console.log(type);
//console.log(type.schema());

/*
var entity = {
   timestamp: '2020-07-16T06:08:07.077Z',
   hit: {
      type: 'pageview',
      propertyId: 'fdsa'
   }
};

const valid = type.isValid(entity);
const clone = type.clone(entity);
console.log('clone: ')
console.log(clone);
const buf2 = type.toBuffer(clone);
const val2 = type.fromBuffer(buf2); 
console.log(val2);
*/

// tests based on common hits in https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#commonhits
describe('#transform()', function() {
    var request = {
        params: {
            topic: 'dummy'
        },
        body: {
            message: {
                attributes : { 
                    name: 'Hit', 
                    namespace: 'com.google.analytics.v1', 
                    timestamp: '2020-07-16T06:08:07.077Z', 
                    topic: 'com.google.analytics.v1.Hit.RAW', 
                    uuid: '63958418-b0f4-4007-b639-fe4698ffe220'
                }
            }
        }
    };

    var headers = {
      host: 'europe-west1-streamprocessor.cloudfunctions.net',
      'user-agent': 'Mozilla/5.0 (X11; CrOS aarch64 13020.87.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.119 Safari/537.36',
      referer: 'https://mydemo.com/',
      'x-appengine-city' : 'ronninge',
      'x-appengine-cityLatLong' : '59.194337,17.748790',
      'x-appengine-country' : 'SE',
      'x-appengine-region' : 'ab',
    };
    var queryString = {};

    var payload = '_v=j84&a=1435566331&_s=1&ul=en-us&de=UTF-8&sd=24-bit&sr=1536x864&vp=728x714&je=0&_u=QACAAIABAAAAAC~&jid=1896139330&gjid=1514327549&_gid=521817103.1594879687&_r=1&gtm=3uf7839Q8B&z=1963138515'

    var expectedTemplate = [
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
                  //name: 'Chrome',
                  //version: '83.0.4103.119',
                  //versionMajor: '83',
                  //engineName: 'Blink',
                  //engineVersion: '83.0.4103.119',
                  javaEnabled: false
               },
               operatingsystem:{
                  encoding: 'UTF-8',
                  language: 'en-us',
                  //name: 'Chromium OS',
                  //version: '13020.87.0'
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
      }
   ];

  
   // https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#page
   it('page tracking with document host (dh) and document page (dp)', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&tid=UA-12345-6&t=pageview&cid=555&dh=mydemo.com&dp=%2Fhome%3Ffoo%3Dbar&dt=homepage'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.content.url = 'mydemo.com/home?foo=bar';
      expectedResult[0].data.content.hostname = 'mydemo.com';
      expectedResult[0].data.content.path = '/home?foo=bar';
      expectedResult[0].data.content.title = 'homepage';
      
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))

   });

   
   it('page tracking with document location (dl)', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&tid=UA-12345-6&t=pageview&cid=555&dl=https%3A%2F%2Fmydemo.com%2Fhome%3Fhello%3Dworld&dt=homepage'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.content.url = 'https://mydemo.com/home?hello=world';
      expectedResult[0].data.content.hostname = 'mydemo.com';
      expectedResult[0].data.content.path = '/home?hello=world';
      expectedResult[0].data.content.title = 'homepage';
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   
   it('page tracking, host (dh) and page (dp) overrides document location (dl)', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&tid=UA-12345-6&t=pageview&cid=555&dl=https%3A%2F%2Fmydemo1.com%2Fhome1%3Fhello%3Dworld&dt=homepage&dh=mydemo2.com&dp=%2Fhome2%3Ffoo%3Dbar'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.content.url = 'mydemo2.com/home2?foo=bar';
      expectedResult[0].data.content.hostname = 'mydemo2.com';
      expectedResult[0].data.content.path = '/home2?foo=bar';
      expectedResult[0].data.content.title = 'homepage';
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   
   it('page tracking with custom dimensions and metrics', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&tid=UA-12345-6&t=pageview&cid=555&dh=mydemo.com&dp=%2Fhome&dt=homepage&cd1=green&cd2=small&cm1=1&cm2=1000'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.content.hostname = 'mydemo.com';
      expectedResult[0].data.content.path = '/home';
      expectedResult[0].data.content.url = 'mydemo.com/home';
      expectedResult[0].data.content.title = 'homepage';
      expectedResult[0].data.customDimensions = {'cd1':'green', 'cd2': 'small'};
      expectedResult[0].data.customMetrics = {'cm1':1, 'cm2': 1000};
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   
   it('page tracking with experiment', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=pageview&tid=UA-12345-6&cid=555&dl=https%3A%2F%2Fmydemo.com%2Fhome&dt=homepage&exp=ST-hSlKmQxyRsYbFHmAi-a.1!D1kUyJlRq0zaFYvv23P8Q.0'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.content.url = 'https://mydemo.com/home';
      expectedResult[0].data.content.hostname = 'mydemo.com';
      expectedResult[0].data.content.path = '/home';
      expectedResult[0].data.content.title = 'homepage';
      expectedResult[0].data.experiments.push({id:'ST-hSlKmQxyRsYbFHmAi-a', variant: '1'});
      expectedResult[0].data.experiments.push({id:'D1kUyJlRq0zaFYvv23P8Q', variant: '0'});
      expect(transformed[0]).to.deep.equal(expectedResult[0]);
      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   // https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#event
   it('event tracking', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=event&tid=UA-12345-6&cid=555&ec=video&ea=play&el=holiday&ev=300'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.event = {
         action : 'play',
         category : 'video',
         label : 'holiday',
         value : 300
      };
      expectedResult[0].data.hit.type = 'event';
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   // https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#exception
   it('exception tracking', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=exception&tid=UA-12345-6&cid=555&exd=IOException&exf=1'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.hit.type = 'exception';
      expectedResult[0].data.exception = {
         description : 'IOException',
         fatal : true
      };
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   // https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#social
   it('social tracking', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=social&tid=UA-12345-6&cid=555&sa=like&sn=facebook&st=%2Fhome'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.hit.type = 'social';
      expectedResult[0].data.social = {
         network : 'facebook',
         action : 'like',
         target : '/home'
      };
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   // https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#usertiming
   it('timing tracking', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=timing&tid=UA-12345-6&cid=555&utc=jsonLoader&utv=load&utt=5000&utl=jQuery&dns=100&pdt=20&rrt=32&tcp=56&srt=12&clt=23&dit=24&plt=25'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.hit.type = 'timing';
      expectedResult[0].data.latency = {
         timing:{
            category : 'jsonLoader',
            label : 'jQuery',
            variable : 'load',
            time : 5000
         },
         domainLookupTime : 100,
         domContentLoadedTime : 23,
         domInteractiveTime : 24,
         pageDownloadTime : 20,
         pageLoadTime : 25,
         redirectionTime : 32,
         serverConnectionTime : 56,
         serverResponseTime : 12
      };
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   // https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#screenView
   it('app/screen tracking', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=screenview&tid=UA-12345-6&cid=555&an=funTimes&av=1.5.0&aid=com.foo.App&aiid=com.android.vending&cd=Home'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.hit.type = 'screenview';
      expectedResult[0].data.app = {
         name : 'funTimes',
         id : 'com.foo.App',
         version : '1.5.0',
         installerId : 'com.android.vending'
      };
      expectedResult[0].data.content.screenName = 'Home';
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   it('traffic source tracking with document host (dh) and document page (dp)', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=pageview&tid=UA-12345-6&cid=555&dh=mydemo.com&dp=%2Fhome%3Futm_source%3Dgoogle%26utm_medium%3Dcpc%26utm_campaign%3Dspring_sale%26utm_term%3Dspring%252Csale%252Crunning%26utm_content%3Drunning_shoe%26utm_id%3D123&dr=https%3A%2F%2Fgoogle.com&gclid=123456&dclid=789&ci=123&cn=spring_sale&cm=cpc&cc=running_shoe&cs=google&ck=spring,sale,running&dt=homepage'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.content.url = 'mydemo.com/home?utm_source=google&utm_medium=cpc&utm_campaign=spring_sale&utm_term=spring%2Csale%2Crunning&utm_content=running_shoe&utm_id=123';
      expectedResult[0].data.content.hostname = 'mydemo.com';
      expectedResult[0].data.content.path = '/home?utm_source=google&utm_medium=cpc&utm_campaign=spring_sale&utm_term=spring%2Csale%2Crunning&utm_content=running_shoe&utm_id=123';
      expectedResult[0].data.content.title = 'homepage';
      expectedResult[0].data.trafficSource = {
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
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   
   it('campaign tracking with document location (dl)', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&tid=UA-12345-6&t=pageview&cid=555&dl=https%3A%2F%2Fmydemo.com%2Fhome%3Futm_source%3Dgoogle%26utm_medium%3Dcpc%26utm_campaign%3Dspring_sale%26utm_term%3Dspring%252Csale%252Crunning%26utm_content%3Drunning_shoe%26utm_id%3D123%26gclid%3D123456%26dclid%3D789&dt=homepage&dr=https%3A%2F%2Fgoogle.com'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.content.url = 'https://mydemo.com/home?utm_source=google&utm_medium=cpc&utm_campaign=spring_sale&utm_term=spring%2Csale%2Crunning&utm_content=running_shoe&utm_id=123&gclid=123456&dclid=789';
      expectedResult[0].data.content.hostname = 'mydemo.com';
      expectedResult[0].data.content.path = '/home?utm_source=google&utm_medium=cpc&utm_campaign=spring_sale&utm_term=spring%2Csale%2Crunning&utm_content=running_shoe&utm_id=123&gclid=123456&dclid=789';
      expectedResult[0].data.content.title = 'homepage';
      expectedResult[0].data.trafficSource = {
         campaign : null,
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
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   it('product action tracking with custom dimensions and metrics', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=event&tid=UA-12345-6&cid=555&ec=UX&ea=click&el=Results&pa=click&pal=Search%20Results&pr1id=P12345&pr1nm=Android%20Warhol%20T-Shirt&pr1ca=Apparel&pr1br=Google&pr1va=Black&pr1ps=1&pr1cd1=small&pr1cd2=green&pr1cm1=1&pr1cm2=1000&pr1pr=29.20&pr1qt=2&pr1cc=SUMMER_SALE13'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.event = {
         action : 'click',
         category : 'UX',
         label : 'Results'
      };
      expectedResult[0].data.hit.type = 'event';
      expectedResult[0].data.ecommerce.products.push({
         action: 'click',
         sku: 'P12345',
         name: 'Android Warhol T-Shirt',
         brand: 'Google',
         variant: 'Black',
         category: 'Apparel',
         couponCode: 'SUMMER_SALE13',
         quantity: 2,
         price: 29.20,
         list: 'Search Results',
         position: 1,
         customDimensions: {
            cd1: 'small',
            cd2: 'green'
         },
         customMetrics: {
            cm1: '1',
            cm2: '1000'
         }
      });
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });
   

   it('product impression list tracking ', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=pageview&tid=UA-12345-6&cid=555&dh=mydemo.com&dp=%2Fhome&dt=homepage&il1nm=Search%20Results&il1pi1id=P12345&il1pi1nm=Android%20Warhol%20T-Shirt&il1pi1ca=Apparel%2FT-Shirts&il1pi1br=Google&il1pi1va=Black&il1pi1ps=1&il1pi1cd1=Member&il2nm=Recommended%20Products&il2pi1nm=Yellow%20T-Shirt&il2pi2nm=Red%20T-Shirt'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.hit.type = 'pageview';
      expectedResult[0].data.content.hostname = 'mydemo.com';
      expectedResult[0].data.content.path = '/home';
      expectedResult[0].data.content.url = 'mydemo.com/home';
      expectedResult[0].data.content.title = 'homepage';
      expectedResult[0].data.ecommerce.products.push({
         action: 'impression',
         sku: 'P12345',
         name: 'Android Warhol T-Shirt',
         brand: 'Google',
         variant: 'Black',
         category: 'Apparel/T-Shirts',
         list: 'Search Results',
         position: 1,
         customDimensions: {
            cd1: 'Member'
         },
         customMetrics: null
      });
      expectedResult[0].data.ecommerce.products.push({
         action: 'impression',
         name: 'Yellow T-Shirt',
         list: 'Recommended Products',
         customDimensions: null,
         customMetrics: null
      });
      expectedResult[0].data.ecommerce.products.push({
         action: 'impression',
         name: 'Red T-Shirt',
         list: 'Recommended Products',
         customDimensions: null,
         customMetrics: null
      });
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   
   https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#combining-impressions-and-actions
   it('product action tracking with custom dimensions and metrics', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=event&tid=UA-12345-6&cid=555&ec=UX&ea=click&el=Results&pa=detail&pr1id=P12345&pr1nm=Android%20Warhol%20T-Shirt&pr1ca=Apparel&pr1br=Google&pr1va=Black&pr1ps=1&il1nm=Related%20Products&il1pi1id=P12345&il1pi1nm=Android%20Warhol%20T-Shirt&il1pi1ca=Apparel%2FT-Shirts&il1pi1br=Google&il1pi1va=Black&il1pi1ps=1'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.hit.type = 'event';
      expectedResult[0].data.event = {
         action : 'click',
         category : 'UX',
         label : 'Results'
      };
      expectedResult[0].data.ecommerce.products.push({
         action: 'impression',
         sku: 'P12345',
         name: 'Android Warhol T-Shirt',
         brand: 'Google',
         variant: 'Black',
         category: 'Apparel/T-Shirts',
         list: 'Related Products',
         position: 1,
         customDimensions: null,
         customMetrics: null
      }); 
      expectedResult[0].data.ecommerce.products.push({
         action: 'detail',
         sku: 'P12345',
         name: 'Android Warhol T-Shirt',
         brand: 'Google',
         variant: 'Black',
         category: 'Apparel',
         position: 1,
         customDimensions: null,
         customMetrics: null
      });
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   // https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#measuring-purchases
   it('product purchase tracking ', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=pageview&tid=UA-12345-6&cid=555&dh=mydemo.com&dp=%2Freceipt&dt=Receipt%20Page&ti=T12345&ta=Google%20Store%20-%20Online&tr=37.39&tt=2.85&ts=5.34&tcc=SUMMER2013&pa=purchase&pr1id=P12345&pr1nm=Android%20Warhol%20T-Shirt&pr1ca=Apparel&pr1br=Google&pr1va=Black&pr1ps=1'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.hit.type = 'pageview';
      expectedResult[0].data.content.hostname = 'mydemo.com';
      expectedResult[0].data.content.path = '/receipt';
      expectedResult[0].data.content.url = 'mydemo.com/receipt';
      expectedResult[0].data.content.title = 'Receipt Page';
      expectedResult[0].data.ecommerce.transaction = {
         affiliation: 'Google Store - Online',
         coupon: 'SUMMER2013',
         id: 'T12345',
         revenue: 37.39,
         shipping: 5.34,
         tax: 2.85
      };
      expectedResult[0].data.ecommerce.products.push({
         action: 'purchase',
         sku: 'P12345',
         name: 'Android Warhol T-Shirt',
         brand: 'Google',
         variant: 'Black',
         category: 'Apparel',
         position: 1,
         customDimensions: null,
         customMetrics: null
      });
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   // https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#measuring-refunds
   it('transaction refund tracking', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=event&tid=UA-12345-6&cid=555&ec=Ecommerce&ea=Refund&ni=1&ti=T12345&pa=refund'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.event = {
         action : 'Refund',
         category : 'Ecommerce'
      };
      expectedResult[0].data.hit.type = 'event';
      expectedResult[0].data.hit.nonInteraction = true;
      expectedResult[0].data.ecommerce.transaction = {
         id: 'T12345',
         refund: true
      };
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   it('single product refund tracking', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=event&tid=UA-12345-6&cid=555&ec=Ecommerce&ea=Refund&ni=1&ti=T12345&pa=refund&pr1id=P12345&pr1qt=1'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.event = {
         action : 'Refund',
         category : 'Ecommerce'
      };
      expectedResult[0].data.hit.type = 'event';
      expectedResult[0].data.hit.nonInteraction = true;
      expectedResult[0].data.ecommerce.transaction = {
         id: 'T12345',
         refund: true
      };
      expectedResult[0].data.ecommerce.products.push({
         action: 'refund',
         sku: 'P12345',
         quantity: 1,
         customDimensions: null,
         customMetrics: null
      });
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#measuring-the-checkout-process
   it('checkout step tracking', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=pageview&tid=UA-12345-6&cid=555&dh=mydemo.com&dp=%2Fcheckout&dt=Checkout&pa=checkout&pr1id=P12345&pr1nm=Android%20Warhol%20T-Shirt&pr1ca=Apparel&pr1br=Google&pr1va=Black&pr1pr=29.20&pr1qt=1&cos=1&col=Visa'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.hit.type = 'pageview';
      expectedResult[0].data.content.hostname = 'mydemo.com';
      expectedResult[0].data.content.path = '/checkout';
      expectedResult[0].data.content.url = 'mydemo.com/checkout';
      expectedResult[0].data.content.title = 'Checkout';
      expectedResult[0].data.ecommerce.checkout = {
         option: 'Visa',
         step: 1
      };
      expectedResult[0].data.ecommerce.products.push({
         action: 'checkout',
         sku: 'P12345',
         name: 'Android Warhol T-Shirt',
         brand: 'Google',
         variant: 'Black',
         category: 'Apparel',
         quantity: 1,
         price: 29.20,
         customDimensions: null,
         customMetrics: null
      });
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   
   it('checkout options tracking', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=event&tid=UA-12345-6&cid=555&ec=Checkout&ea=Option&pa=checkout_option&cos=2&col=FedEx'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.event = {
         action : 'Option',
         category : 'Checkout'
      };
      expectedResult[0].data.hit.type = 'event';
      expectedResult[0].data.ecommerce.checkout = {
         option: 'FedEx',
         step: 2
      };
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });


   // https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#promotion-impressions
   it('Promotion Impressions tracking', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=pageview&tid=UA-12345-6&cid=555&dh=mydemo.com&dp=%2Fhome&dt=homepage&promo1id=PROMO_1234&promo1nm=Summer%20Sale&promo1cr=summer_banner2&promo1ps=banner_slot1'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.hit.type = 'pageview';
      expectedResult[0].data.content.hostname = 'mydemo.com';
      expectedResult[0].data.content.path = '/home';
      expectedResult[0].data.content.url = 'mydemo.com/home';
      expectedResult[0].data.content.title = 'homepage';
      expectedResult[0].data.ecommerce.promotions.push({
         action: 'impression',
         id: 'PROMO_1234',
         name: 'Summer Sale',
         position: 'banner_slot1',
         creative: 'summer_banner2' 
      });
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

      expect(type.isValid(type.clone(transformed[0].data), { // test that data is valid according to avro schema
         errorHook(path, any, type) {
         console.error(`'${any}' is not a valid value (of type ${type}) for '${path.join(".")}'`);
         }
      })).to.be.true;
      const clone = type.clone(transformed[0].data);
      expect(clone).to.deep.equal(type.fromBuffer(type.toBuffer(clone))); // test that data serialized and then deserialized is equal to original data
      //console.log(type.fromBuffer(type.toBuffer(clone)))
   });

   // https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#promotion-clicks
   it('Promotion click tracking', function() {
      var data = {};
      data.payload = payload.concat('&', 'v=1&t=event&tid=UA-12345-6&cid=555&ec=Internal%20Promotions&ea=click&el=Summer%20Sale&promoa=click&promo1id=PROMO_1234&promo1nm=Summer%20Sale&promo1cr=summer_banner2&promo1ps=banner_slot1'); 
      data.headers = headers;
      data.queryString = queryString;
      request.body.message.data = Buffer.from(JSON.stringify(data)).toString('base64');
      var transformed = adapter.transform(request);
      var expectedResult = _.cloneDeep(expectedTemplate);
      expectedResult[0].attributes.uuid = transformed[0].attributes.uuid;
      expectedResult[0].data.event = {
         action : 'click',
         category : 'Internal Promotions',
         label : 'Summer Sale'
      };
      expectedResult[0].data.hit.type = 'event';
      expectedResult[0].data.ecommerce.promotions.push({
         action: 'click',
         id: 'PROMO_1234',
         name: 'Summer Sale',
         position: 'banner_slot1',
         creative: 'summer_banner2' 
      });
      expect(transformed[0]).to.deep.equal(expectedResult[0]); // test message result

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