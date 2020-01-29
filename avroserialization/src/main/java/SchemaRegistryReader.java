import com.google.gson.internal.LinkedTreeMap;
import feign.Feign;
import feign.FeignException;
import feign.gson.GsonDecoder;
import feign.gson.GsonEncoder;
import feign.okhttp.OkHttpClient;
import org.apache.avro.Schema;

/**
 * Class to connect to Schema Registry and retrive schema using the schema id.
 */
public class SchemaRegistryReader {
    public Schema getSchemaFromID(String registryURL, String schemaID) {
        SchemaRegistryClient registryClient = Feign.builder()
                .client(new OkHttpClient())
                .encoder(new GsonEncoder())
                .decoder(new GsonDecoder())
                .target(SchemaRegistryClient.class, registryURL);
        LinkedTreeMap returnedSchema = (registryClient.findByID(schemaID));
        String jsonSchema = returnedSchema.get("schema").toString();
        return new Schema.Parser().parse(jsonSchema);
    }

    public static void connectToSchemaRegistry(String url) throws FeignException {
        SchemaRegistryClient registryClient = Feign.builder()
                .client(new OkHttpClient())
                .encoder(new GsonEncoder())
                .decoder(new GsonDecoder())
                .target(SchemaRegistryClient.class, url);
        registryClient.connect();
    }
}
