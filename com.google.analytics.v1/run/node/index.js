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
const app = express();
const avro = require('avsc');

const {PubSub} = require('@google-cloud/pubsub');
const pubsub = new PubSub();

const { Storage } = require('@google-cloud/storage')
const storage = new Storage()

//const uuidv4 = require('uuid/v4');

const queryStringParser = require('query-string');
const urlParser = require('url');
const parser = require('ua-parser-js');

const _ = require('lodash');

const topic = process.env.TOPIC;
const schemaBucket = storage.bucket(process.env.SCHEMA_BUCKET || 'streamprocessor-demo-schemas-38b3d09');
const schemaName = process.env.SCHEMA_NAME || 'com.google.analytics.v1.Entity.avsc';
const searchEnginesPattern = process.env.SEARCH_ENGINES_PATTERN || '/.*(www\.google\.|www\.bing\.|search\.yahoo\.).*/';
const ignoredReferersPattern = process.env.IGNORED_REFERERS_PATTERN || '';
const socialNetworksPattern = process.env.SOCIAL_NETWORKS_PATTERN || '/.*(facebook\.|instagram\.|youtube\.|linkedin\.|twitter\.).*/';
const siteSearchPattern = process.env.SITE_SEARCH_PATTERN || '.*q=(([^&#]*)|&|#|$)';
const excludedUserAgentsPattern = process.env.EXCLUDED_USER_AGENTS_PATTERN || '/(?!)/';

//console.log(schemaBucket);
console.log(schemaName);

// Create an Express object

//let avroString = '{}';
let registry = {}; // Registry where new types get added.
let unionType, inputType, outputType;

const readJsonFromFile = async remoteFilePath => new Promise((resolve, reject) => {
    console.log('hello');
    let buf = ''
    schemaBucket.file(remoteFilePath)
      .createReadStream()
      .on('data', d => (buf += d))
      .on('end', () => resolve(buf))
      .on('error', e => reject(e))
  })

app.set('trust proxy', true);
app.post('/', apiPost);
exports.com_google_analytics_v1 = app;

async function processing(request){
     // get avro schema if not already cached
     if(typeof inputType === 'undefined' || typeof outputType === 'undefined'){
        await readJsonFromFile(schemaName).then(function(avroString){
            console.log(avroString);
            unionType = avro.Type.forSchema(JSON.parse(avroString), {registry});
            inputType = registry['com.google.analytics.v1.transformed.Entity'];
            outputType = registry['com.google.analytics.v1.processed.Entity'];
        })
    } else{
        console.log('schema is cached');
    }

    //console.log(request.body.message.data);
    console.log(Buffer.from(request.body.message.data, 'base64'));
    console.log(inputType);
    console.log(outputType);

    /*** START processing ***/ 

    const inputData = inputType.fromBuffer(Buffer.from(request.body.message.data, 'base64'));
    if(!inputData.device.userAgent.match(excludedUserAgentsPattern)){
        const attributes = request.body.message.attributes;
    
        var message = {};
        
        message.attributes = typeof attributes  !== 'undefined' ?  Object.assign({}, attributes)  : {};
        message.attributes.namespace = 'com.google.analytics.v1.processed';
        message.attributes.name = 'Entity';

        console.log(inputData);

        var outputData = inputData;

        // Site search logic
        if(inputData.content.url.match(siteSearchPattern)){
            outputData.content.searchKeyword = inputData.content.url.match(siteSearchPattern)[1];
        }

        // Traffic source logic
        if(typeof inputData.trafficSource.gclid !== 'undefined'){
            outputData.trafficSource.source = 'google search ads';
            outputData.trafficSource.medium = 'cpc';
        }else if(typeof inputData.trafficSource.dclid !== 'undefined'){
            outputData.trafficSource.source = 'google display & video';
            outputData.trafficSource.medium = 'cpm';
        }else if(typeof inputData.trafficSource.utm.source !== 'undefined'){
            outputData.trafficSource.source = inputData.trafficSource.utm.source;
            outputData.trafficSource.medium = inputData.trafficSource.utm.medium;
            outputData.trafficSource.name = inputData.trafficSource.utm.name;
            outputData.trafficSource.content = inputData.trafficSource.utm.content;
        }

        
        /*** STOP transformation ***/
        message.data = outputType.toBuffer(outputData);
        return message;
    }else{
        console.info('excluded user agent');
    }
}

async function publish(request, response){
   

    //process request
    var message = await processing(request); 
    
    // Publish to topic
    if(typeof message !== 'undefined'){
        try{
            let messageId =  await pubsub
                .topic(topic)
                .publish(message.data, message.attributes);
            console.log(messageId);
        } catch(err) {
            console.error(err.message);
            response.status(400).end(`error when publishing data object to pubsub`);
        };
    }
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