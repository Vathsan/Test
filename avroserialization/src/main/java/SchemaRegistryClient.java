import com.google.gson.internal.LinkedTreeMap;
import feign.Headers;
import feign.Param;
import feign.RequestLine;

/**
 * This interface defines the http calls to schema registry.
 */
public interface SchemaRegistryClient {
    @RequestLine("GET")
    Object connect();

    @RequestLine("GET /schemas/ids/{id}")
    @Headers("Content-Type: application/json")
    LinkedTreeMap findByID(@Param("id") String id);
}