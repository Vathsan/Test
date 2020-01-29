import org.apache.avro.Schema;

public class Main {
    public static void main(String[] args) {
        SchemaRegistryReader reader = new SchemaRegistryReader();
        Schema schema = reader.getSchemaFromID("http://localhost:8081/", "1");

        String jsonString = "{\"firstName\":\"Stephania\",\"lastName\":\"Okuneva\",\"birthDate\":582023554621}";

        AvroMessageProcessor pro = new AvroMessageProcessor();
        byte[] serialized = pro.serializeAvroMessage(jsonString, schema);
        System.out.println("Serialized " + serialized);

        Object deSerialized = pro.deserializeByteArray(serialized, schema);
        System.out.println("Deserialized " + deSerialized);
    }
}
