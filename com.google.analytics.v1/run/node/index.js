/*
 * Copyright (c) 2020 Robert Sahlin
 *
 * Use of this software is governed by the Business Source License 1.1.
 * 
 * Parameters
 * 
 * Licensor:             Robert Sahlin
 * Licensed Work:        StreamProcessor
 *                       The Licensed Work is (c) 2020 Robert Sahlin.
 * Additional Use Grant: You may use the Licensed Work when the Licensed Work is 
 *                       processing less than 10 Million unique events per month, 
 *                       provided that you do not use the Licensed Work for a 
 *                       commercial offering that allows third parties to access
 *                       the functionality of the Licensed Work so that such third
 *                       parties directly benefit from the features of the Licensed Work.
 * 
 * Change Date:          12 months after the git commit date of the code
 * 
 * Change License:       GNU AFFERO GENERAL PUBLIC LICENSE, Version 3
 * 
 * For information about alternative licensing arrangements for the Licensed Work,
 * please contact the licensor.
 */

'use strict';

const express = require('express');

const {PubSub} = require('@google-cloud/pubsub');
const pubsub = new PubSub();

const uuidv4 = require('uuid/v4');

const queryStringParser = require('query-string');
const urlParser = require('url');
const parser = require('ua-parser-js');

const _ = require('lodash');
const topic = process.env.TOPIC;
const schemaBucket = process.env.SCHEMA_BUCKET;

// Create an Express object
const app = express();
app.set('trust proxy', true);
app.post('/', apiPost);
exports.com_google_analytics_v1 = app;

// get schema
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

const registry = {}; // Registry where new types get added.
const unionType = avro.Type.forSchema(JSON.parse(avroString), {registry});
const inputType = registry['com.google.analytics.v1.transformed.Entity'];
const outputType = registry['com.google.analytics.v1.Entity'];


function processing(request){
    const inputData = inputType.fromBuffer(Buffer.from(request.body.message.data, 'base64'));
    const attributes = request.body.message.attributes;

    /*** START processing ***/ 
    var message = {};
    
    message.attributes = typeof attributes  !== 'undefined' ?  Object.assign({}, attributes)  : {};
    message.attributes.namespace = 'com.google.analytics.v1';
    message.attributes.name = 'Entity';    

    var outputData = inputData;
    
    /*** STOP transformation ***/
    message.data = outputType.toBuffer(outputData);
    return message;
}

async function publish(request, response){

    //process request
    var messages = processing(request); 
    
    // Publish to topic
    let messageIds = await Promise.all(
        messages.map(async message => { 
            return pubsub
                .topic(topic)
                .publish(message.data, message.attributes)
        }))
        .catch(function(err) {
            console.error(err.message);
            response.status(400).end(`error when publishing data object to pubsub`);
        });
    console.log(messageIds);
}

// Collect pubsub push request (POST), transform it and publish data on pubsub
async function apiPost(request, response) {
    if (!request.body) {
        const errorMsg = 'no Pub/Sub message received';
        console.error(`error: ${errorMsg}`);
        response.status(400).end(`Bad Request: ${errorMsg}`);
    }
    if (!request.body.message) {
        const errorMsg = 'invalid Pub/Sub message format';
        console.error(`error: ${errorMsg}`);
        response.status(400).end(`Bad Request: ${errorMsg}`);
    }
    if (!request.body.message.data) {
        const errorMsg = 'invalid Pub/Sub message data';
        console.error(`error: ${errorMsg}`);
        response.status(400).end(`Bad Request: ${errorMsg}`);
    }
    try{
        await publish(request, response);
        if(!response.headersSent){
            response.status(204).end();
            //response.status(200).json(transform(request));
        }
    }catch (error) {
        console.error(error);
        if(!response.headersSent){
            response.status(400).end();
        }
    }
}

if(typeof exports !== 'undefined') {
    exports.processing = processing;
}