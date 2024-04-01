import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.elasticsearch.client.RestClient;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.cluster.HealthResponse;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

public class ElasticJavaRestClientHttps {
	public static void main(String[] args) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException {
		
		ElasticsearchClient esClient = getElasticsearchConnection();
		
		//createIndex(esClient);

		//createDocuments(esClient);
		
		searchRecords(esClient);

	}

	private static void searchRecords(ElasticsearchClient esClient) throws IOException {
		GetRequest.Builder getRequestBuilder = new GetRequest.Builder();
        getRequestBuilder.index("simplifying_tech_idx");
        getRequestBuilder.id("201");
 
        GetRequest getRequest = getRequestBuilder.build();
 
        GetResponse<Employee> getResponse = esClient.get(getRequest, Employee.class);
 
        Employee employee = getResponse.source();
 
        System.out.println(employee.getEmplId());
        System.out.println(employee.getName());
        System.out.println(employee.getDept());
	}

	private static void createDocuments(ElasticsearchClient esClient) throws IOException {
		Employee employee = new Employee();
        employee.setEmplId(201);
        employee.setName("Jane Smith");
        employee.setDept("HR");
 
        IndexRequest.Builder<Employee> indexReqBuilder = new IndexRequest.Builder<>();
 
        indexReqBuilder.index("simplifying_tech_idx");
        indexReqBuilder.id(employee.getEmplId().toString());
        indexReqBuilder.document(employee);
        IndexRequest<Employee> indexRequest = indexReqBuilder.build();
 
        IndexResponse response = esClient.index(indexRequest);
 
        System.out.println("Indexed with version " + response.version());
	}

	private static void createIndex(ElasticsearchClient esClient) throws IOException {
		CreateIndexRequest.Builder createIndexBuilder = new CreateIndexRequest.Builder();
        createIndexBuilder.index("simplifying_tech_idx");
        CreateIndexRequest createIndexRequest = createIndexBuilder.build();
 
        ElasticsearchIndicesClient indices = esClient.indices();
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest);
 
        System.out.println("Index Created Successfully: "+createIndexResponse.acknowledged());
	}

	private static ElasticsearchClient getElasticsearchConnection() throws NoSuchAlgorithmException, KeyManagementException,
			KeyStoreException, CertificateException, IOException {
		String path = "C:\\Tools\\elasticsearch-8.10.2\\config\\certs\\truststore.p12";
        File certFile = new File(path);
         
        //SSLContext --  secure socket protocolimplementation which acts as a factory 
        //SSLContextBuilder -- Java Security Standard Algorithm Names Specification
        SSLContext sslContext = SSLContextBuilder.create().loadTrustMaterial(certFile, "password".toCharArray()).build();
 
        //Abstract credentials provider that maintains a collection of usercredentials
        BasicCredentialsProvider credsProv = new BasicCredentialsProvider(); 
        credsProv.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic","XXXXXXXXXXXXX"));
         
        //Client that connects to an Elasticsearch
        RestClient restClient = RestClient
            .builder(new HttpHost("localhost", 9200, "https")) 
            .setHttpClientConfigCallback(hc -> hc
                .setSSLContext(sslContext) 
                .setDefaultCredentialsProvider(credsProv)
            )
            .build();
 
        // Create the transport and the API client
        //A transport layer that implements Elasticsearch specificities
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        ElasticsearchClient client = new ElasticsearchClient(transport);
         
        //to check Elastic server health and connection status
        HealthResponse healthResponse = client.cluster().health();
        System.out.printf("Elasticsearch status is: [%s]", healthResponse.status());
        
        return client;
	}
}
