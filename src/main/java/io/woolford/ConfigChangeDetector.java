package io.woolford;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.flipkart.zjsonpatch.JsonDiff;
import com.sendgrid.*;
import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.logging.Logger;

@Component
public class ConfigChangeDetector {

    private static Logger logger = Logger.getLogger(ConfigChangeDetector.class.getName());

    @Value("${ambari.protocol}")
    private String ambariProtocol;

    @Value("${ambari.host}")
    private String ambariHost;

    @Value("${ambari.port}")
    private Integer ambariPort;

    @Value("${ambari.user}")
    private String ambariUser;

    @Value("${ambari.pass}")
    private String ambariPass;

    @Value("${ambari.cluster.name}")
    private String ambariClusterName;

    @Value("${sendgrid.apikey}")
    private String sendgridApiKey;

    @Value("${sendgrid.from.email}")
    private String sendgridFromEmail;

    @Value("${sendgrid.from.name}")
    private String sendgridFromName;

    @Value("${sendgrid.to.email}")
    private String sendgridToEmail;

    @Value("${sendgrid.to.name}")
    private String sendgridToName;

    @Scheduled(cron = "*/20 * * * * *")
    void captureChanges() throws URISyntaxException, IOException {

        CloseableHttpClient httpclient = HttpClients.createDefault();
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(ambariUser + ":" + ambariPass));
        HttpClientContext localContext = HttpClientContext.create();
        localContext.setCredentialsProvider(credentialsProvider);

        URI uri = new URIBuilder()
                .setScheme(ambariProtocol)
                .setHost(ambariHost)
                .setPort(ambariPort)
                .setPath("/api/v1/clusters/" + ambariClusterName + "/configurations/service_config_versions")
                .build();

        HttpGet httpget = new HttpGet(uri);

        String encoding = Base64.getEncoder().encodeToString((ambariUser + ":" + ambariPass).getBytes());
        httpget.addHeader("X-Requested-By", ambariUser);
        httpget.setHeader("Authorization", "Basic " + encoding);

        CloseableHttpResponse closeableHttpResponse = httpclient.execute(httpget, localContext);
        HttpEntity responseEntity = closeableHttpResponse.getEntity();

        String previousServiceConfigVersions = new String(Files.readAllBytes(Paths.get("/tmp/service-config-versions.json")));

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        String response;
        if(responseEntity!=null) {
            response = EntityUtils.toString(responseEntity);

            JsonNode serviceConfigVersionsJson = objectMapper.readTree(response);
            JsonNode previousServiceConfigVersionsJson = objectMapper.readTree(previousServiceConfigVersions);

            JsonNode diff = JsonDiff.asJson(previousServiceConfigVersionsJson, serviceConfigVersionsJson);
            if (diff.size() > 0){
                logger.info(objectMapper.writeValueAsString(diff));
                sendMail("Ambari config change", objectMapper.writeValueAsString(diff));
            }

            try(  PrintWriter out = new PrintWriter( "/tmp/service-config-versions.json" )  ){
                out.println( objectMapper.writeValueAsString(serviceConfigVersionsJson) );
            }

        }

        closeableHttpResponse.close();

    }

    private void sendMail(String subject, String contentValue) throws IOException {

        SendGrid sendgrid = new SendGrid(sendgridApiKey);

        Email from = new Email();
        from.setEmail(sendgridFromEmail);
        from.setName(sendgridFromName);

        Email to = new Email();
        to.setEmail(sendgridToEmail);
        to.setName(sendgridToName);

        Content content = new Content("text/plain", contentValue);
        Mail mail = new Mail(from, subject, to, content);

        Request request = new Request();

        try {
            request.setMethod(Method.POST);
            request.setEndpoint("mail/send");
            request.setBody(mail.build());
            Response response = sendgrid.api(request);
            logger.info(String.valueOf(response.getStatusCode()));
            logger.info(response.getBody());
            logger.info(response.getHeaders().toString());
        } catch (IOException ex) {
            logger.warning(ex.getMessage());
            throw ex;
        }

    }

}
