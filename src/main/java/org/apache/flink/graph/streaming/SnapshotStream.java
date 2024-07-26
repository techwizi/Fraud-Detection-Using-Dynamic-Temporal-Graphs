/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.streaming;

import java.util.Iterator;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
 * A stream of discrete graphs, each maintaining
 * the graph state of the edges contained in the respective window.
 * It is created by calling {@link SimpleEdgeStream#slice()}.
 * The graph slice is keyed by the source or target vertex of the edge stream,
 * so that all edges of a vertex are in the same tumbling window.
 *
 * @param <K> the vertex ID type
 * @param <EV> the edge value type
 */
public class SnapshotStream<K, EV> {

	private WindowedStream<Edge<K, EV>, K, TimeWindow> windowedStream;

	SnapshotStream(WindowedStream<Edge<K, EV>, K, TimeWindow> window) {
		this.windowedStream = window;
	}

	/**
	 * Performs a neighborhood aggregation on the graph window stream.
	 *
	 * @param aggregateFunction
	 * @return the result stream after applying the user-defined aggregation operation on the window
	 */
	public <T> DataStream<T> aggregateNeighbors(final EdgesAggregateFunction<K, EV, T> aggregateFunction) {
		return windowedStream.aggregate(new EdgesAggregateFunction<>(aggregateFunction));
	}

	@SuppressWarnings("serial")
	public static final class EdgesAggregateFunction<K, EV, T>
			implements AggregateFunction<Edge<K, EV>, T, T>, ResultTypeQueryable<T> {

		private final EdgesAggregateFunction<K, EV, T> aggregateFunction;

		public EdgesAggregateFunction(EdgesAggregateFunction<K, EV, T> aggregateFunction) {
			this.aggregateFunction = aggregateFunction;
		}

		@Override
		public T createAccumulator() {
			return aggregateFunction.createAccumulator();
		}

		@Override
		public T add(Edge<K, EV> value, T accumulator) {
			return aggregateFunction.add(value, accumulator);
		}

		@Override
		public T getResult(T accumulator) {
			return accumulator;
		}

		@Override
		public T merge(T a, T b) {
			return aggregateFunction.merge(a, b);
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(EdgesAggregateFunction.class, aggregateFunction.getClass(), 2,
					null, null);
		}
	}

	/**
	 * Performs an aggregation on the neighboring edges of each vertex on the graph window stream.
	 * <p>
	 * For each vertex, the transformation consecutively calls a
	 * {@link EdgesReduce} function until only a single value for each edge remains.
	 * The {@link EdgesReduce} function combines two edge values into one new value of the same type.
	 *
	 * @param reduceFunction the aggregation function
	 * @return a result stream of Tuple2, containing one tuple per vertex.
	 * The first field is the vertex ID and the second field is the final value,
	 * after applying the user-defined aggregation operation on the neighborhood.
	 */
	public DataStream<Tuple2<K, EV>> reduceOnEdges(final EdgesReduce<EV> reduceFunction) {
		return windowedStream.reduce(new EdgesReduceFunction<>(reduceFunction))
				.project(0, 2);
	}

	@SuppressWarnings("serial")
	public static final class EdgesReduceFunction<K, EV> implements ReduceFunction<Edge<K, EV>> {

		private final EdgesReduce<EV> reduceFunction;

		public EdgesReduceFunction(EdgesReduce<EV> reduceFunction) {
			this.reduceFunction = reduceFunction;
		}

		@Override
		public Edge<K, EV> reduce(Edge<K, EV> firstEdge, Edge<K, EV> secondEdge) throws Exception {
			EV reducedValue = this.reduceFunction.reduceEdges(firstEdge.getValue(), secondEdge.getValue());
			firstEdge.setValue(reducedValue);
			return firstEdge;
		}
	}

	/**
	 * Performs a generic neighborhood aggregation in the graph window stream.
	 * Each vertex can produce zero, one or more values from the computation on its neighborhood.
	 *
	 * @param applyFunction the neighborhood computation function
	 * @return the result stream after applying the user-defined operation on the window
	 */
	public <T> DataStream<T> applyOnNeighbors(final EdgesApply<K, EV, T> applyFunction) {
		return windowedStream.apply(new SnapshotFunction<>(applyFunction));
	}

	@SuppressWarnings("serial")
	public static final class SnapshotFunction<K, EV, T> implements
			WindowFunction<Edge<K, EV>, T, K, TimeWindow>, ResultTypeQueryable<T> {

		private final EdgesApply<K, EV, T> applyFunction;

		public SnapshotFunction(EdgesApply<K, EV, T> applyFunction) {
			this.applyFunction = applyFunction;
		}

		public void apply(K key, TimeWindow window, final Iterable<Edge<K, EV>> edges, Collector<T> out)
				throws Exception {

			final Iterator<Tuple2<K, EV>> neighborsIterator = new Iterator<Tuple2<K, EV>>() {

				final Iterator<Edge<K, EV>> edgesIterator = edges.iterator();

				@Override
				public boolean hasNext() {
					return edgesIterator.hasNext();
				}

				@Override
				public Tuple2<K, EV> next() {
					Edge<K, EV> nextEdge = edgesIterator.next();
					return new Tuple2<>(nextEdge.f1, nextEdge.f2);
				}

				@Override
				public void remove() {
					edgesIterator.remove();
				}
			};

			Iterable<Tuple2<K, EV>> neighborsIterable = new Iterable<Tuple2<K, EV>>() {
				public Iterator<Tuple2<K, EV>> iterator() {
					return neighborsIterator;
				}
			};

			applyFunction.applyOnEdges(key, neighborsIterable, out);
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(EdgesApply.class, applyFunction.getClass(), 2,
					null, null);
		}
	}
}