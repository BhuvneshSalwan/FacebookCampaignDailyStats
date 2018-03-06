package com.google.bigquery.main;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;

public class GAuthenticate {

	public static final HttpTransport TRANSPORT = new NetHttpTransport();
	public static final JsonFactory JSON_FACTORY = new JacksonFactory();
	
	public static Bigquery getAuthenticated(){
		
		Bigquery bigquery = null;
		
		if((bigquery = createAuthorizedClient()) != null){
			return bigquery;
		}
		else{
			return null;
		}
	
	}
	
	public static Bigquery createAuthorizedClient(){

		GoogleCredential credential = null;
		
		if((credential = authorize()) != null){
			return new Bigquery.Builder(TRANSPORT, JSON_FACTORY, credential).build();
		}
		else{
			return null;
		}
		
	}
	
	public static GoogleCredential authorize(){
	
		try{

			GoogleCredential credential = GoogleCredential.fromStream(new FileInputStream("C:\\Program Files\\Java\\projects\\FacebookCampaignDailyStats\\target\\Cloud Project 1-a5efe977981f.json"))
				    .createScoped(BigqueryScopes.all());
			
			return credential;

		}
		catch(Exception e){
			System.out.println("Exception : Authenticate - Authorize Method" );
			System.out.println(e);
			return null;
		}
	
	}
	
}