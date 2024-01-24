package cl.fala.functions;

import java.util.Date;

import com.github.sisyphsu.dateparser.DateParserUtils;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class GetDateTimeEpochMillis {
    @UserFunction("promodef.getMilliseconds")
    @Description ("promodef.getMilliseconds(DateTime:String)::Long")
    public Long getMilliseconds(
        @Name("strings") String DateTime
    ) {
        Date date = DateParserUtils.parseDate(DateTime);
        return date.getTime();
    }

}
