import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# Specify a different encoding
df = pd.read_csv(r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\SW-Tax\USC Marking Analysis\IRM Data\IRM_AssessmentComponent_20240926.csv", encoding='ISO-8859-1')

# DATA QUALITY

#1. Check for missing values

print("Number of missing values by column: ")
print(df.isnull().sum())


#2. Check for duplicates 

print("Number of duplicated rows: ")
print(df.duplicated().sum())


# DATA STRUCTURE AND TYPES

#check the shape of the data
print("Data set shape No. Rows : No. Columns ")
print(df.shape)

#check the data types by column
print("Data Types by Column: ")
print(df.dtypes)

#Return the number of each data type
print("Number of columns by each data type: ")
print(df.dtypes.value_counts())



#OUTLIERS

# Min Max check

def find_extremes(df, column):
    min_value = df[column].min()
    max_value = df[column].max()
    return min_value, max_value

#Check by column for any extreme values
min_val, max_val = find_extremes(df, 'Weight')
print(f"Minimum: {min_val}, Maximum: {max_val}")


#Find the 5 smallest values

print("5 smallest values within the column")
smallest_values = df['Weight'].nsmallest(5)
print("Smallest values:\n", smallest_values)


# Find the 5 largest values
print("5 largest values within the column")
largest_values = df["Weight"].nlargest(5)
print("largest values\n", largest_values)


#find the 5 smallest unqiue values
print("5 smallest unique values within the column")
smallest_Unique_values = df['Weight'].sort_values().unique()[:5]
print("Smallest Unique values:\n", smallest_Unique_values)


#find the 5 largest unqiue values 
print("5 largest unique values within the column")
largest_Unique_values = df['Weight'].sort_values(ascending=False).unique()[:5]
print("Largest Unique values:\n",largest_Unique_values)


#DATA DISTRIBUTIONS & SUMMARY STATISTICS


def calucate_statistics(df, column):
    '''
    Returns the mean, median, and mode for a specified column in the DataFrame. 

    Parameters:
        df (pd.DataFrame) : The DataFrame containing the data. 
        column (str): The column name to caculate statistic for. 

    Returns:
        dict: A dictionary containing the mean, median, and mode of the column.

    '''
    mean_value = df[column].mean()
    median_value = df[column].median()
    mode_value = df[column].mode().iloc[0] if not df[column].mode().empty else None

    return {
        'mean' : mean_value,
        'median' : median_value,
        'mode' : mode_value
    }

print(calucate_statistics(df, 'Weight'))

def plot_histogram_seaborn(df, column, bins=10):
    '''
    Creates a histogram for a specified column using seaborn. 

    Parameters:
        df (pd.DataFrame): The DataFrame containing the data. 
        column (str): The column name to create a hisogram for. 
        bins (int): The number of bins in the histogram (default is 10).
    '''

    plt.figure(figsize=(10, 6))
    sns.histplot(df[column].dropna(), bins=bins, kde=True, edgecolor='black')
    plt.xlabel(column)
    plt.ylabel('Frequency')
    plt.xlabel(f'Hisogram of {column}')
    plt.show()

plot_histogram_seaborn(df, 'Weight', bins=10)


#Example code on how to exclude an outlier from the data frame

#plot_histogram_seaborn(df[df.Weight != 3.75], 'Weight', bins=10)


print(df.columns)