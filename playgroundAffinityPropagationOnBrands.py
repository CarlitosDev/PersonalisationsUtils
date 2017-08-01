import redshiftSqlAlchemy as rsa
import pandas as pd
import pandasql as pdsql
import joinMetaDefinitions as jn
import os
import carlosUtils as cu

import bokehUtils as bk

# some ML
from sklearn.cluster import AffinityPropagation
from sklearn import metrics
from sklearn.preprocessing import LabelEncoder

# Authenticate with Redshift using your db credentials.
user     = 'carlos_aguilar'
password = 'MdogDI64j6vH90g973'
dbname   = 'adform'
host     = 'adform-ops.c7dxcjhlundm.eu-central-1.redshift.amazonaws.com'

rs = rsa.RedshiftAlchemy(user=user, password=password, 
    database=dbname, host=host)


xlsRoot   = '/Users/carlos.aguilar/Google Drive/PythonDev/Beamly';




# JUST ONE BRAND
# Let's try Wella Professional
clientId = '133070'


sqlQuery = '''SELECT DISTINCT
yyyy_mm_dd,
publisher_domain,
destination_url,
browsername,
devicename,
regioncode,
matchingimpression
FROM adform.impressionsClicksMapped
WHERE client_id = {}
order by yyyy_mm_dd ASC'''.format(clientId)

df = rs.query2DF(sqlQuery)

fName     = clientId + '.pickle';
xlsFile   = os.path.join(xlsRoot, fName)
#df.to_pickle(xlsFile)


objTypes   = df.select_dtypes(include=['object']).keys().tolist()
catVarName = []
# one hot encoding
for varName in objTypes:
    currentVarName = varName+'CAT'
    catVarName.append(currentVarName)
    df[currentVarName] = LabelEncoder().fit_transform(df[varName])

npArray = df[catVarName].as_matrix();

varCorrespondence = dict(zip(objTypes, catVarName))


# headless chickem approach - don't use labels
af = AffinityPropagation(preference=-50).fit(npArray)
cluster_centers_indices = af.cluster_centers_indices_
labels = af.labels_

n_clusters_ = len(cluster_centers_indices)
print('Estimated number of clusters: %d' % n_clusters_)


#print("Homogeneity: %0.3f" % metrics.homogeneity_score(labels_true, labels))
#print("Completeness: %0.3f" % metrics.completeness_score(labels_true, labels))
#print("V-measure: %0.3f" % metrics.v_measure_score(labels_true, labels))
#print("Adjusted Rand Index: %0.3f"
#      % metrics.adjusted_rand_score(labels_true, labels))
#print("Adjusted Mutual Information: %0.3f"
#      % metrics.adjusted_mutual_info_score(labels_true, labels))
#print("Silhouette Coefficient: %0.3f"
#      % metrics.silhouette_score(X, labels, metric='sqeuclidean'))


# Plot result
import matplotlib.pyplot as plt
from itertools import cycle

plt.close('all')
plt.figure(1)
plt.clf()

colors = cycle('bgrcmykbgrcmykbgrcmykbgrcmyk')
for k, col in zip(range(n_clusters_), colors):
    class_members = labels == k
    cluster_center = npArray[cluster_centers_indices[k]]
    plt.plot(npArray[class_members, 0], npArray[class_members, 1], col + '.')
    plt.plot(cluster_center[0], cluster_center[1], 'o', markerfacecolor=col,
             markeredgecolor='k', markersize=14)
    for x in npArray[class_members]:
        plt.plot([cluster_center[0], x[0]], [cluster_center[1], x[1]], col)

plt.title('Estimated number of clusters: %d' % n_clusters_)
plt.show()


rs.close()
