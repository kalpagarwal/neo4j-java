package cl.fala.functions;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.UUID;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import cl.fala.utilities.UUIDGenerator;

public class GenerateUUID {
    @UserFunction("promodef.generateUUIDV5")
    @Description("promodef.generateUUIDV5(['id', 'tenant']) - generate v5 uuid.")
    public String generateUUIDV5(
            @Name("strings") List<String> fields,
            @Name("nameSpace_URL") String nameSpace
        ) throws UnsupportedEncodingException {
        if (fields == null || fields.isEmpty()) {
            return null;
        }
        nameSpace =  nameSpace.isEmpty() ? "6ba7b811-9dad-11d1-80b4-00c04fd430c8" : nameSpace; 
        String name = String.join("-", fields);
        UUID uuidv5 = UUIDGenerator.generateType5UUID(nameSpace, name);
        return uuidv5.toString();
    }

    @UserFunction("promodef.generateUUIDV4")
    @Description("promodef.generateUUIDV4() - generate random uuid.")
    public String generateUUIDV4() {
        UUID uuidv4 = UUIDGenerator.generateType4UUID();
        return uuidv4.toString();
    }


    @UserFunction("promodef.generateUUID")
    @Description("promodef.generateUUID - generate uuid v4 or v5.")
    public String generateUUID(
        @Name("strings") List<String> fields,
        @Name("nameSpace_URL") String nameSpace
    ) throws UnsupportedEncodingException {
        if (fields != null && !fields.isEmpty()) {
            return generateUUIDV5(fields,nameSpace);
        } else {
            return generateUUIDV4();
        }
    }
}
