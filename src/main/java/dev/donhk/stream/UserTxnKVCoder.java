package dev.donhk.stream;

import dev.donhk.pojos.UserTxn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.KV;

import java.io.*;
import java.util.Collections;
import java.util.List;

public class UserTxnKVCoder extends Coder<KV<Long, UserTxn>> {

    public static UserTxnKVCoder of() {
        return new UserTxnKVCoder();
    }

    private UserTxnKVCoder() {
    }

    @Override
    public void encode(KV<Long, UserTxn> value, OutputStream outStream) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(value);
            out.flush();
            byte[] yourBytes = bos.toByteArray();
            outStream.write(yourBytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public KV<Long, UserTxn> decode(InputStream inStream) {
        try {
            ObjectInputStream ois = new ObjectInputStream(inStream);
            Object object = ois.readObject();
            return (KV<Long, UserTxn>) object;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        throw new NonDeterministicException(this, "Custom UserTxn coder is not deterministic");
    }
}
