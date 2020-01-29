import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * This class to contain methods to deserialize byte array to Avro Record and serialize
 * json string to byte array.
 */
public class AvroMessageProcessor {
    private static final Logger log = Logger.getLogger(AvroMessageProcessor.class);

    private Schema getAvroSchema(String schemaDefinition, String schemaRegistryURL, String schemaID) {
        Schema schema = null;
            if (schemaDefinition != null) {
                schema = new Schema.Parser().parse(schemaDefinition);
            } else if (schemaRegistryURL != null) {
                SchemaRegistryReader reader = new SchemaRegistryReader();
                schema = reader.getSchemaFromID(schemaRegistryURL, schemaID);
            }
            return schema;
    }

    public static byte[] serializeAvroMessage(String jsonString, Schema schema) {
        Decoder decoder;
        GenericRecord datum;
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        try {
            decoder = DecoderFactory.get().jsonDecoder(schema, jsonString);
            datum = reader.read(null, decoder);
            writer.write(datum, encoder);
            encoder.flush();
            output.close();
            return output.toByteArray();
        } catch (IOException e) {
            log.error("IOException occured when serializing event " + jsonString +
                    " to avro message of schema " + schema.toString());
            return null;
        }
    }

    public static Object deserializeByteArray(byte[] data, Schema schema) {
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        try {
            Object record = reader.read(null, decoder);
            return record;
        } catch (IOException e) {
            log.error("Error occured when deserializing avro byte stream conforming " +
                    "to schema " + schema.toString() + ". Hence dropping the event.");
            return null;
        }
    }
}
