package com.netflix.aegisthus.io.writable;

import com.netflix.Aegisthus;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AegisthusKeyMapper extends Mapper<AegisthusKey, AtomWritable, AegisthusKey, AtomWritable> {
    private AbstractType<?> rowKeyComparator;
    
    @Override protected void setup(
            Context context)
            throws IOException, InterruptedException {
        super.setup(context);

        String rowKeyType = context.getConfiguration().get(Aegisthus.Feature.CONF_KEYTYPE, "BytesType");

        try {
            rowKeyComparator = TypeParser.parse(rowKeyType);
        } catch (SyntaxException | ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void map(AegisthusKey key, AtomWritable value, Context context)
            throws IOException, InterruptedException {
        try {
            String[] keyWhitelist = context.getConfiguration().getStrings(Aegisthus.Feature.CONF_KEY_WHITELIST);
            if (keyWhitelist != null && keyWhitelist.length > 0) {
                String readableKey = rowKeyComparator.getString(key.getKey());
                boolean matches = false;
                for (String prefix : keyWhitelist) {
                    if (readableKey.startsWith(prefix + "\\:")) matches = true;
                }
                if (!matches) return;
            }
            context.write(key, value);
        } catch (MarshalException | IllegalArgumentException ignored) {
            // don't emit key
        }
    }
}
