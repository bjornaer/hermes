package vector

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/bjornaer/hermes/internal/disk/types"
	imath "github.com/bjornaer/hermes/internal/math"
)

const (
	minDataPointsRequired = 2
	DefaultBuckets        = 10.0
)

const (
	cosineMetricsMaxIteration      = 200
	cosineMetricsMaxTargetSample   = 100
	cosineMetricsTwoMeansThreshold = 0.7
	cosineMetricsCentroidCalcRatio = 0.0001
)

// GetNormalVector calculates the normal vector of a hyperplane that separates
// the two clusters of data points.
// nolint: funlen, gocognit, cyclop, gosec
func GetNormalVector[T comparable](dataPoints []*types.DataPoint[T], dm DistanceMeasure, NumberOfDimensions int) []float64 {
	lvs := len(dataPoints)
	// Initialize two centroids randomly from the data points.
	c0, c1 := getRandomCentroids[T](dataPoints)

	// Repeat the two-means clustering algorithm until the two clusters are
	// sufficiently separated or a maximum number of iterations is reached.
	for i := 0; i < cosineMetricsMaxIteration; i++ {
		// Create a map from cluster ID to a slice of vectors assigned to that
		// cluster during clustering.
		clusterToVecs := map[int][][]float64{}

		// Randomly sample a subset of the data points.
		iter := imath.Min(cosineMetricsMaxTargetSample, len(dataPoints))

		// Assign each of the sampled vectors to the cluster with the nearest centroid.
		for i := 0; i < iter; i++ {
			v := dataPoints[rand.Intn(len(dataPoints))].Embedding
			ip0 := dm.CalcDistance(c0, v)
			ip1 := dm.CalcDistance(c1, v)

			if ip0 > ip1 {
				clusterToVecs[0] = append(clusterToVecs[0], v)
			} else {
				clusterToVecs[1] = append(clusterToVecs[1], v)
			}
		}

		// Calculate the ratio of data points assigned to each cluster. If the
		// ratio is below a threshold, the clustering is considered to be
		// sufficiently separated, and the algorithm terminates.
		lc0 := len(clusterToVecs[0])
		lc1 := len(clusterToVecs[1])

		if (float64(lc0)/float64(iter) <= cosineMetricsTwoMeansThreshold) &&
			(float64(lc1)/float64(iter) <= cosineMetricsTwoMeansThreshold) {
			break
		}

		// If one of the clusters has no data points assigned to it, re-initialize
		// the centroids randomly and continue.
		if lc0 == 0 || lc1 == 0 {
			c0, c1 = getRandomCentroids[T](dataPoints)

			continue
		}

		// Update the centroids based on the data points assigned to each cluster
		c0 = make([]float64, NumberOfDimensions)
		it0 := int(float64(lvs) * cosineMetricsCentroidCalcRatio)

		for i := 0; i < it0; i++ {
			for d := 0; d < NumberOfDimensions; d++ {
				c0[d] += clusterToVecs[0][rand.Intn(lc0)][d] / float64(it0)
			}
		}

		c1 = make([]float64, NumberOfDimensions)
		it1 := int(float64(lvs)*cosineMetricsCentroidCalcRatio + 1)

		for i := 0; i < int(float64(lc1)*cosineMetricsCentroidCalcRatio+1); i++ {
			for d := 0; d < NumberOfDimensions; d++ {
				c1[d] += clusterToVecs[1][rand.Intn(lc1)][d] / float64(it1)
			}
		}
	}

	// Create a new array to hold the resulting normal vector.
	ret := make([]float64, NumberOfDimensions)

	// Calculate the normal vector by subtracting the coordinates of the second centroid from those of the first centroid.
	// Store the resulting value in the corresponding coordinate of the ret slice.
	for d := 0; d < NumberOfDimensions; d++ {
		v := c0[d] - c1[d]
		ret[d] += v
	}

	return ret
}

// nolint: gosec
func getRandomCentroids[T comparable](dataPoints []*types.DataPoint[T]) ([]float64, []float64) {
	lvs := len(dataPoints)
	k := rand.Intn(lvs)
	l := rand.Intn(lvs - 1)

	if k == l {
		l++
	}

	c0 := dataPoints[k].Embedding
	c1 := dataPoints[l].Embedding

	return c0, c1
}

func ConvertFloat64ArrToStr(flts []float64) string {
	s := ""
	for _, f := range flts {
		s += fmt.Sprintf("%v$", f)
	}
	return s
}

func ConvertStrToEmbedding(s string) ([]float64, error) {
	spl := strings.Split(s, "$")
	var embed []float64
	for _, v := range spl {
		if v == "" {
			continue
		}

		f64, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return embed, err
		}
		embed = append(embed, f64)
	}
	return embed, nil
}
