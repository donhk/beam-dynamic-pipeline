package dev.donhk.stream;

import dev.donhk.pojos.CarInformation;
import dev.donhk.pojos.ElasticRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class CarInfo2ElasticRow extends PTransform<PCollection<KV<String, CarInformation>>, PCollection<KV<String, ElasticRow>>> {
    public static CarInfo2ElasticRow of() {
        return new CarInfo2ElasticRow();
    }

    @Override
    public PCollection<KV<String, ElasticRow>> expand(PCollection<KV<String, CarInformation>> input) {
        //car_id,car_model,car_make,city,car_time,cost,promo
        return input.apply(MapElements.into(TypeDescriptors.kvs(
                        TypeDescriptors.strings(),
                        TypeDescriptor.of(ElasticRow.class)))
                .via((SerializableFunction<KV<String, CarInformation>, KV<String, ElasticRow>>) input1 -> {
                            final ElasticRow row = ElasticRow.create();
                            final CarInformation txn = input1.getValue();
                            row.addCol("ID", txn.getId());
                            row.addCol("CAR_TIME", txn.getCarTime());
                            row.addCol("CAR_MAKE", txn.getCarMake());
                            row.addCol("MODEL", txn.getCarModel());
                            row.addCol("CITY", txn.getCity());
                            row.addCol("COST", txn.getCost());
                            row.addCol("PROMO", txn.getPromo());
                            return KV.of(input1.getKey(), row);
                        }
                ));
    }
}
