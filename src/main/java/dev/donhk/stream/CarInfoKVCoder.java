package dev.donhk.stream;

import dev.donhk.pojos.CarInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.KV;

import java.io.*;
import java.util.Collections;
import java.util.List;

public class CarInfoKVCoder extends Coder<KV<String, CarInformation>> {

    public static CarInfoKVCoder of() {
        return new CarInfoKVCoder();
    }

    private CarInfoKVCoder() {
    }

    @Override
    public void encode(KV<String, CarInformation> value, OutputStream outStream) {
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
    public KV<String, CarInformation> decode(InputStream inStream) {
        try {
            ObjectInputStream ois = new ObjectInputStream(inStream);
            Object object = ois.readObject();
            return (KV<String, CarInformation>) object;
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
        throw new NonDeterministicException(this, "Custom CarInfoKVCoder coder is not deterministic");
    }
}